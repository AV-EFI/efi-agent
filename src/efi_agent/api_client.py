import pathlib
import urllib.parse as urlparse

import appdirs
from avefi_schema import model as efi
from linkml_runtime.loaders import json_loader
from linkml_runtime.utils.formatutils import remove_empty_items
import requests
from requests import auth
from requests.exceptions import HTTPError, JSONDecodeError
import yaml

CONFIG_DIR = pathlib.Path(appdirs.user_config_dir(appname=__package__))


class ApiError(HTTPError):
    @classmethod
    def from_http_error(cls, e):
        try:
            msg = f"{e}: {e.response.json()}"
        except JSONDecodeError:
            msg = f"{e}: {e.response.text}"
        return cls(msg, response=e.response)


class EpicApi(requests.Session):
    EFI_BASE_CLASS = efi.MovingImageRecord
    KIP = \
        'http://typeapi.lab.pidconsortium.net/v1/types/schema/' \
        '21.T11969/873d5c9f6ebbffecf1df'
    PURGE_SLOTS = [
        ('has_identifier',),
        ('has_source_key',),
        ('described_by', 'last_modified'),
    ]

    def __init__(self, profile, prefix, suffix=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.suffix = suffix
        with open(profile) as f:
            self.profile = yaml.safe_load(f)
        credentials_path = CONFIG_DIR / 'credentials.yml'
        with credentials_path.open() as f:
            credentials = yaml.safe_load(f)
        for creds in credentials:
            if creds['prefix'] == prefix:
                break
        else:
            raise RuntimeError(f"Did not find credentials for prefix {prefix}")
        base_url = creds['base_url']
        if base_url.endswith('/'):
            self.base_url = base_url
        else:
            self.base_url = f"{base_url}/"
        self.auth = auth.HTTPBasicAuth(creds['username'], creds['password'])

    def create(self, efi_record):
        url = self.prefix
        if self.suffix:
            url += f"?suffix={self.suffix}"
        r = self.request('POST', url, efi_record=efi_record)
        return r

    def update(self, pid, efi_record):
        r = self.request('PUT', pid, efi_record=efi_record)
        return r

    def get(self, pid):
        r = self.request('GET', pid)
        return r

    def efi_from_response(self, response):
        if response.request.method in ('POST', 'PUT'):
            pid = response.json().get('handle')
            values = response.json().get('values')
        elif response.request.method == 'GET':
            if not response.url.startswith(f"{self.base_url}{self.prefix}"):
                raise RuntimeError(
                    f"URL in response does not start with {self.base_url}:"
                    f" {response.url}")
            pid = urlparse.urlsplit(response.url[len(self.base_url):]).path
            values = response.json()
        else:
            pid = None
            values = None
        if values:
            if values[0].get('parsed_data', {}).get('value') != self.KIP:
                raise ValueError(
                    f"Handle not compliant with KIP {self.KIP} ({pid})")
            efi_record = json_loader.loads(
                values[1]['parsed_data']['value'], self.EFI_BASE_CLASS)
            efi_record.has_identifier.append(efi.AVefiResource(id=pid))
            efi_record.described_by.last_modified = efi.ISODate(
                values[2]['timestamp'])
        else:
            efi_record = None
        return pid, efi_record

    def handle_from_efi(self, efi_record):
        if not isinstance(efi_record, self.EFI_BASE_CLASS):
            raise ValueError(
                f"efi_record must be of type {self.EFI_BASE_CLASS} but is"
                f" {type(efi_record)} instead")
        if isinstance(efi_record, efi.WorkVariant):
            efi_record.described_by = None
        else:
            efi_record.described_by = efi.DescriptionResource(**self.profile)
        efi_dict = remove_empty_items(efi_record, hide_protected_keys=True)
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
        }]

    def request(self, method, relative_url, efi_record=None, **kwargs):
        url = urlparse.urljoin(self.base_url, relative_url)
        if efi_record and not kwargs.get('json'):
            kwargs['json'] = self.handle_from_efi(efi_record)
        r = super().request(
            method, url, auth=self.auth, **kwargs)
        try:
            r.raise_for_status()
        except HTTPError as e:
            raise ApiError.from_http_error(e) from e
        return r
