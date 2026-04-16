# -*- coding: utf-8 -*-

import json
import logging
import re
from uuid import uuid4

from avefi_schema import model_pydantic_v2 as efi
from httpx import Client, HTTPStatusError, Timeout

from .config import get_credentials, settings


log = logging.getLogger(__name__)


class EpicClient(Client):
    EFI_BASE_CLASS = efi.MovingImageRecord
    KIP = \
        'http://typeapi.lab.pidconsortium.net/v1/types/schema/' \
        '21.T11969/873d5c9f6ebbffecf1df'
    PURGE_SLOTS = [
        ('has_identifier',),
    ]
    www_prefix = 'https://www.av-efi.net/res/'

    def __init__(
            self, profile: dict, prefix: str, suffix: str | None = None,
            timeout: Timeout | None = None, **kwargs):
        if timeout is None:
            timeout = Timeout(settings.CONNECTION_TIMEOUT)
        super().__init__(timeout=timeout, **get_credentials(prefix), **kwargs)
        self.prefix = prefix
        self.suffix = suffix
        self.profile = profile

    def create(self, efi_record: efi.MovingImageRecord):
        pid = f"{self.prefix}/{str(uuid4()).upper()}"
        if self.suffix:
            pid += f"-{self.suffix}"
        return self._put(pid, efi_record, preexisting=False)

    def update(self, pid: str, efi_record: efi.MovingImageRecord):
        return self._put(pid, efi_record)

    def _put(
            self, pid: str, efi_record: efi.MovingImageRecord,
            preexisting: bool = True):
        payload = self.handle_from_efi(pid, efi_record)
        headers = {}
        if not preexisting:
            headers = {'If-None-Match': '*'}
        log.info(f"pid: {pid}, headers: {headers}\npayload: {payload}")
        return self.request('PUT', pid, headers=headers, json=payload)

    def get(self, pid: str):
        return self.request('GET', pid)

    def efi_from_response(
            self, response, unvalidated_json=False
    ) -> tuple[str, efi.MovingImageRecord]:
        if response.request.method in ('POST', 'PUT'):
            pid = response.json().get('handle')
            values = response.json().get('values')
        elif response.request.method == 'GET':
            if not str(response.url).startswith(
                    f"{self.base_url}{self.prefix}"):
                raise RuntimeError(
                    f"URL in response does not start with {self.base_url}:"
                    f" {response.url}")
            pid = str(response.url)[len(str(self.base_url)):]
            values = response.json()
        else:
            pid = None
            values = None
        if values:
            if values[0].get('parsed_data', {}) != self.KIP:
                raise ValueError(
                    f"Handle not compliant with KIP {self.KIP} ({pid})")
            if unvalidated_json:
                efi_record = json.loads(values[1]['parsed_data'])
                efi_record.get('has_identifier', []).append(
                    efi.AVefiResource(id=pid).model_dump())
            else:
                efi_record = efi.MovingImageRecordTypeAdapter.validate_json(
                    values[1]['parsed_data'])
                efi_record.has_identifier.append(efi.AVefiResource(id=pid))
        else:
            efi_record = None
        return pid, efi_record

    def handle_from_efi(
            self, pid: str, efi_record: efi.MovingImageRecord) -> dict:
        if not isinstance(efi_record, self.EFI_BASE_CLASS):
            raise ValueError(
                f"efi_record must be of type {self.EFI_BASE_CLASS} but is"
                f" {type(efi_record)} instead")
        described_by_issuer(efi_record, self.profile)
        apply_fixes(efi_record)
        efi_dict = efi_record.model_dump(mode='json', exclude_none=True)
        for key_seq in self.PURGE_SLOTS:
            dict_ptr = efi_dict
            try:
                for key in key_seq[:-1]:
                    dict_ptr = dict_ptr[key]
                del dict_ptr[key_seq[-1]]
            except KeyError:
                pass
        return [{
            'type': 'KIP',
            'parsed_data': self.KIP,
            'idx': 1,
        },{
            'type': 'has_record',
            'parsed_data': efi_dict,
            'idx': 2,
        },{
            'type': 'URL',
            'parsed_data': f"{self.www_prefix}{pid[len(self.prefix)+1:]}",
            'idx': 3,
        }]

    def request(self, method, relative_url, efi_record=None, **kwargs):
        r = super().request(method, relative_url, **kwargs)
        # Evil hack trying to mitigate spurious server errors
        if r.status_code == 500:
            log.warning(
                f"Server sent {r.status_code} in response to"
                f" {r.request.method} on {r.url}, retrying once")
            r = super().request(method, relative_url, **kwargs)
        try:
            return r.raise_for_status()
        except HTTPStatusError as e:
            try:
                msg = f"{e}\nResponse body:\n{e.response.json()}"
            except json.JSONDecodeError:
                msg = f"{e}\nResponse body:\n{e.response.text}"
            log.error(msg)
            raise


def described_by_issuer(
        record: efi.MovingImageRecord, issuer: dict
) -> efi.DescriptionResource:
    """Return described_by entry matching ``issuer``.

    Get the DescriptionResource entry of ``record`` matching
    ``issuer`` if present and create it if not. Note that described_by
    is multivalued for WorkVariant records only. Therefore, ValueError
    will be raised for Manifestation or Item records that alreadey
    have a value for described_by that does not match ``issuer``.

    """
    if isinstance(record, efi.WorkVariant):
        for described_by in record.described_by or []:
            if described_by.has_issuer_id == issuer['has_issuer_id']:
                break
        else:
            record.described_by = [efi.DescriptionResource(**issuer)]
            described_by = record.described_by[0]
    else:
        if record.described_by:
            described_by = record.described_by
            if described_by.has_issuer_id != issuer['has_issuer_id']:
                raise ValueError(
                    f"Cannot add source_key {source_key} by issuer_id"
                    f" {issuer.has_issuer_id} to record from issuer"
                    f" {described_by.has_issuer_id}")
        else:
            record.described_by = efi.DescriptionResource(**issuer)
            described_by = record.described_by
    return described_by


def apply_fixes(efi_record):
    """Fix common mistakes, so they will not cause a schema
    violation.

    Make sure all has_duration.has_value fields actually comply with
    the AVefi schema. Further fixes may be added.

    Note, this function modifies the supplied record in place.

    """
    if isinstance(efi_record, efi.Item) and efi_record.has_duration:
        orig_duration = efi_record.has_duration.has_value
    else:
        orig_duration = None
    if orig_duration and not re.search(
            r'^PT[1-9]*[0-9][0-9]H[0-5][0-9]M[0-5][0-9]S$', orig_duration):
        m = re.search(
            r'^PT((?P<H>[0-9]+)H)?((?P<M>[0-9]+)M)?((?P<S>[0-9]+)S)?$',
            orig_duration)
        if not m:
            raise ValueError(f"Cannot parse duration: {orig_duration}")
        m_groups = m.groupdict()
        seconds = int(m_groups['S'] or '0') + 60 * (
            int(m_groups['M'] or '0') + 60 * int(m_groups['H'] or '0'))
        hours = seconds // 3600
        seconds -= hours * 3600
        minutes = seconds // 60
        seconds -= minutes * 60
        efi_record.has_duration.has_value = \
            f"PT{hours:0>2}H{minutes:0>2}M{seconds:0>2}S"
