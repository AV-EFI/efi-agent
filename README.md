License: MIT
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# efi-agent

Register PIDs according to the [AVefi schema][] from the command line.

## Usage

```console
$ pip install "efi-agent @ git+https://github.com/AV-EFI/efi-agent.git"
## [...]
$ efi-agent push --help
Usage: efi-agent push [OPTIONS] INPUT_FILE

  Push AVefi records to the handle system, updating or creating PIDs.
  
Options:
  -j, --journal FILE  Journal where actions on PIDs are to be recorded.
  -p, --profile FILE  Profile for the described_by slot in the AVefi schema.
  --prefix TEXT       Prefix to be used when generating handles.
  --suffix TEXT       Suffix to be used when generating handles.
  --help              Show this message and exit.
```

The profile parameter determines who will be recorded as data provider
in the described_by slots of the generated PIDs. It should be a JSON
file similar to this:

```yaml
{
  "has_issuer_id": "https://w3id.org/isil/DE-89",
  "has_issuer_name": "Technische Informationsbibliothek (TIB)",
}
```

This is specified as part of the [AVefi schema][].

[AVefi schema]: https://av-efi.github.io/av-efi-schema/
