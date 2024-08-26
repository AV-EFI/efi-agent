from collections import defaultdict
import enum
import graphlib
import json
import pathlib

from avefi_schema import model as efi
from linkml_runtime.loaders import json_loader


class Operations(enum.Enum):
    CREATE = 1
    GET = 2
    UPDATE = 3


class Scheduler:
    """Manage the tasks required to sync a batch of AVefi moving image
    records with the handle system.

    Register a bunch of records via the ``add_record()`` method.
    Processing the lot can be initiated by calling the ``submit()``
    method.

    Since multiple manifestations can be linked to the same work, any
    manifestation may be linked to multiple works, and works may be
    linked to other works themselves (e.g. episodes of a series),
    dependencies need to be calculated and observed. The following
    will be taken care of::

    -   For each reference expressed using identifiers of local scope,
        the corresponding record with tha identifier is actually part of
        the batch.
    -   For all references like is_part_of, is_manifestation_of, etc.,
        identifiers of local scope are replaced by PIDs as soon as they
        are known.
    -   Tasks are performed in an order that ensures that all references
        can be consistently expressed via PIDs by the time a record
        actually hits the handle system.

    Parameters
    ----------
    client : .api_client.EpicApi
        High level interface to ePIC.
    result_log_file : str or pathlib.Path
        Path to a file resulting records will be written to for
        documentation purposes.

    """

    def __init__(self, client, journal_file, input_file=None):
        self.client = client
        self.journal_file = pathlib.Path(journal_file)
        self.result_log = defaultdict(list)
        self.handler_lookup = {}
        self.handlers = []
        self.referencing = defaultdict(list)
        if input_file:
            self.load_from_file(input_file)

    def load_from_file(self, input_file):
        efi_records = json_loader.load_any(
            str(input_file), efi.MovingImageRecord)
        for record in efi_records:
            self.add_record(record)

    def add_record(self, record):
        """Pass in a moving image record for submission to handle system.

        Register a record for further processing. Note that this class
        assumes ownership of all records registered via this method.
        Changes are deliberately made during processing, e.g. updating
        references as new PIDs are registered.

        Parameters
        ----------
        record : efi.MovingImageRecord
            Actually, a subclass like WorkVariant or Item to be included
            in the batch.

        """
        handler = Handler(self, record=record)
        self.handlers.append(handler)

        # Make sure we are notified when references have to be updated
        for attr_name, id in handler.iter_links():
            self.add_reference(handler, attr_name, id)

    def add_reference(self, handler, attr_name, record_id):
        def _raise_on_loop(handler, seen=[]):
            for key in handler.iter_hashable_ids():
                for check_handler, check_attr in self.referencing.get(key, []):
                    if check_attr == attr_name:
                        if check_handler in seen:
                            raise ValueError(
                                f"{attr_name} loop detected while processing"
                                f" {record_id}")
                        seen.append(handler)
                        raise_on_loop(check_handler, seen=seen)

        _raise_on_loop(handler)
        self.referencing[record_id].append((handler, attr_name))

    def record_reverse_dependencies(self):
        """Make handlers aware of down-stream references.

        Make sure that handlers will be triggered to update references
        whenever a PID for some dependency becomes available.

        """
        for key, refs in self.referencing.items():
            handler = self.handler_lookup.get(key)
            if not handler:
                if key[0] == 'avefi:LocalResource':
                    raise ValueError(
                        f"Unresolveable reference to {key} in input data")
                for ref_handler, attr_name in refs:
                    if attr_name == 'is_item_of' \
                       and ref_handler.tasks.get(Operations.CREATE):
                        handler = Handler(
                            self, pid=key[1],
                            source_key=ref_handler.source_key)
                        self.handlers.insert(0, handler)
            if handler:
                handler.referenced_by.extend(refs)
                if handler.pid and key[0] != 'avefi:AVefiResource':
                    handler.update_references()

    def prepare_dependency_graph(self):
        """Create dependency graph for all tasks known to the scheduler.

        Iterate over all handlers and record dependencies between the
        associated tasks in a graph for topological sorting. This is
        necessary because both, multiple manifestations may be linked
        to the same work, but one manifestation may be linked to
        multiple works as well.

        Additionally, take care that a manifestations's has_item
        attribute is updated when new items have been added.

        Returns
        -------
        dict of sets
            Graph representing dependdencies between tasks

        """
        self.record_reverse_dependencies()
        graph = defaultdict(set)
        for handler in self.handlers:
            if not handler.record:
                continue
            dependencies = set()
            for attr_name, id in handler.iter_links():
                if attr_name == 'is_item_of' and not handler.pid:
                    item_create_task = handler.tasks.get(Operations.CREATE)
                else:
                    item_create_task = None
                dep = self.handler_lookup.get(id)
                if not dep:
                    if not item_create_task:
                        continue
                    
                dep_create_task = dep.tasks.get(Operations.CREATE)
                if dep_create_task:
                    dependencies.add(dep_create_task)
                dep_update_task = dep.tasks.get(Operations.UPDATE)
                if item_create_task and not dep_update_task:
                    dep_update_task = dep.add_task(Operations.UPDATE)
                    graph[dep_update_task].add(item_create_task)
                elif dep_update_task:
                    dependencies.add(dep_update_task)
            for task in handler.tasks.values():
                graph[task].update(dependencies)
        return graph

    def submit(self):
        sorter = graphlib.TopologicalSorter(self.prepare_dependency_graph())
        # Make sure we have the right permissions
        with self.journal_file.open('a+') as f:
            pass
        try:
            for task in sorter.static_order():
                task.execute()
        finally:
            self.write_pid_journal()

    def write_pid_journal(self):
        result_list = []
        for source_key, handlers in self.result_log.items():
            actions = []
            for handler in handlers:
                action = None
                for op in (Operations.UPDATE, Operations.CREATE):
                    task = handler.tasks.get(op)
                    if task and task.done:
                        action = task.operation.name
                if not action:
                    continue
                entry = {
                    'action': action,
                    'pid': handler.pid,
                }
                if handler.record:
                    entry['record_type'] = \
                        handler.record.__class__.__name__
                actions.append(entry)
            if actions:
                result_list.append({
                    'source_key': list(source_key),
                    'actions': actions,
                })
        if result_list:
            with self.journal_file.open('a+') as f:
                json.dump(result_list, f, indent=2)
                f.write('\n')


class Task:
    def __init__(self, operation, handler):
        self.operation = operation
        self.handler = handler
        self.name = (*handler.name, operation)
        self.client = handler.scheduler.client
        self.done = False

    def __hash__(self):
        return hash(self.name)

    def execute(self):
        handler = self.handler
        if self.operation == Operations.CREATE:
            r = self.client.create(handler.record)
            handler.pid, handler.record = self.client.efi_from_response(r)
        else:
            # must be an update then
            if not handler.record \
               or isinstance(handler.record, efi.Manifestation):
                if not handler.tasks.get(Operations.CREATE):
                    r = self.client.get(handler.pid)
                    pid, old_record = self.client.efi_from_response(r)
                    # Todo: Update references if PID has become an alias
                    if pid != handler.pid:
                        raise RuntimeError(
                            f"PID for manifestation changed from {handler.pid}"
                            f" to {pid}")
                    if not handler.record:
                        handler.record = old_record
                    has_item = old_record.has_item
                else:
                    has_item = []
                for item_handler in _filter_by(
                        handler.referenced_by, 'is_item_of'):
                    item_id = efi.AVefiResource(id=item_handler.pid)
                    if item_id in has_item:
                        continue
                    # We do not expect alias PIDs for items, so leave it at that
                    has_item.append(item_id)
                handler.record.has_item = has_item
            r = self.client.update(handler.pid, handler.record)
            # Todo: Update references if PID has become an alias
            handler.pid, handler.record = self.client.efi_from_response(r)
        result_log = handler.scheduler.result_log[tuple(handler.source_key)]
        if handler not in result_log:
            result_log.append(handler)
        self.done = True


def _filter_by(required_by_list, attr_name):
    for handler, attr in required_by_list:
        if attr == attr_name:
            yield handler


class Handler:
    """Care taker for one record / PID

    An instance of this class owns all scheduled tasks related to a
    single PID.

    Attributes
    ----------
    record : efi.MovingImageRecord | None
        A record whose PID needs to be created or updated.
    pid : str | None
        The persistent identifier as a string.
    tasks : Dict[Task]
        The tasks scheduled for ``pid``.

    """
    def __init__(self, scheduler, record=None, pid=None, source_key=None):
        self.scheduler = scheduler
        self.source_key = source_key
        self.record = record
        self.referenced_by = []
        self.tasks = {}
        if record:
            if not source_key and record.has_source_key:
                self.source_key = record.has_source_key
            self.name = next(self.iter_hashable_ids())
            for key in self.iter_hashable_ids():
                if key in scheduler.handler_lookup:
                    raise RuntimeError(
                        f"Multiple records with the same identifier: {key}")
                scheduler.handler_lookup[key] = self
                if key[0] == 'avefi:AVefiResource':
                    if not pid:
                        pid = key[1]
                    elif key[1] != pid:
                        raise ValueError(
                            f"Two PIDs provided for the same record"
                            f" ({pid}, {key[1]})")
        else:
            self.name = ('avefi:AVefiResource', pid)
        if pid:
            if isinstance(record, efi.WorkVariant):
                raise NotImplementedError(
                    "Update of a Work/Variant is not implemented yet ({pid})")
            operation = Operations.UPDATE
            self._pid = pid
        else:
            operation = Operations.CREATE
            self._pid = None
        self.add_task(operation)

    def add_task(self, operation):
        task = Task(operation, self)
        self.tasks[operation] = task
        return task

    def __hash__(self):
        return hash(self.name)

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        self._pid = value
        self.update_references()

    def update_references(self):
        def _get_pid(record_id):
            dep = self.scheduler.handler_lookup.get(hashable_id(record_id))
            if dep and dep.pid \
               and (dep.pid != record_id.id \
                    or not isinstance(record_id, efi.AVefiResource)):
                return efi.AVefiResource(id=dep.pid)
            return None

        for handler, attr_name in self.referenced_by:
            attr = getattr(handler.record, attr_name)
            if isinstance(attr, list):
                for idx, identifier in enumerate(attr):
                    new_ref = _get_pid(identifier)
                    if new_ref:
                        attr.pop(idx)
                        attr.insert(idx, new_ref)
            elif attr:
                new_ref = _get_pid(attr)
                if new_ref:
                    setattr(handler.record, attr_name, new_ref)

    def iter_hashable_ids(self):
        for identifier in self.record.has_identifier:
            yield hashable_id(identifier)

    def iter_links(self):
        if isinstance(self.record, efi.WorkVariant):
            link_attributes = ('is_part_of', 'is_variant_of')
        elif isinstance(self.record, efi.Manifestation):
            # Ignore has_item here and deal with that later
            link_attributes = ('is_manifestation_of', 'same_as')
        elif isinstance(self.record, efi.Item):
            link_attributes = ('is_item_of', 'is_copy_of')
        else:
            raise ValueError(
                f"Cannot handle {type(self.record)} (record={self.record})")
        for attr_name in link_attributes:
            attr = getattr(self.record, attr_name)
            if attr is None:
                attr = []
            elif not isinstance(attr, list):
                attr = [attr]
            for identifier in attr:
                yield (attr_name, hashable_id(identifier))


def hashable_id(identifier: efi.AuthorityResource):
    return (identifier.category, identifier.id)