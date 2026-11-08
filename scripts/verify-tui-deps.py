#!/usr/bin/env python3
"""Validate metadata and reconstruct the native replacements from pinned archives."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / 'apps/tui/third_party'
MODULES = {'tea': 'charm.land/bubbletea/v2', 'uv': 'github.com/charmbracelet/ultraviolet'}


def validate(manifest, downloads, patch):
    if hashlib.sha256(patch).hexdigest() != manifest['patch_sha256']:
        raise ValueError('patch_sha256 does not match native-input.patch')
    entries = manifest['modules']
    if len(entries) != len(MODULES) or {e['directory'] for e in entries} != set(MODULES):
        raise ValueError('manifest must name exactly the tea and uv replacements')
    for entry in entries:
        name = entry['directory']
        if entry['module'] != MODULES[name]:
            raise ValueError(f'{name}: incorrect module path')
        upstream = downloads[name]
        for key, actual in [('version', upstream['Version']), ('upstream_sum', upstream['Sum']),
                            ('revision', upstream['Origin']['Hash'])]:
            if entry[key] != actual:
                raise ValueError(f'{name}: {key} does not match verified upstream metadata')
        if entry['license'] != 'MIT':
            raise ValueError(f'{name}: incorrect license metadata')
        changed = set(entry['changed_files'])
        if len(changed) != len(entry['changed_files']) or any(Path(f).is_absolute() or '..' in Path(f).parts for f in changed):
            raise ValueError(f'{name}: invalid changed_files')


def negative_checks(manifest, downloads, patch):
    for field in ['patch_sha256', 'upstream_sum', 'revision', 'version', 'module', 'license']:
        corrupt = copy.deepcopy(manifest)
        target = corrupt if field == 'patch_sha256' else corrupt['modules'][0]
        target[field] = 'incorrect'
        try:
            validate(corrupt, downloads, patch)
        except ValueError:
            continue
        raise AssertionError(f'accepted corrupt {field}')


def main():
    manifest = json.loads((SOURCE / 'manifest.json').read_text())
    patch = (SOURCE / 'native-input.patch').read_bytes()
    downloads = {}
    declared = (ROOT / 'apps/tui/go.mod').read_text().splitlines()
    for name, module in MODULES.items():
        versions = [line.split()[1] for line in declared if line.strip().startswith(module + ' ')]
        if len(versions) != 1:
            raise ValueError(f'{module}: expected one version in app go.mod')
        raw = subprocess.check_output(['go', 'mod', 'download', '-json', module + '@' + versions[0]], cwd=ROOT)
        downloads[name] = json.loads(raw)
    validate(manifest, downloads, patch)
    negative_checks(manifest, downloads, patch)
    with tempfile.TemporaryDirectory(prefix='canti-native-deps-') as temporary:
        temp = Path(temporary)
        for name, upstream in downloads.items():
            shutil.copytree(upstream['Dir'], temp / name, ignore=shutil.ignore_patterns('.github'))
            for path in [temp / name, *(temp / name).rglob('*')]:
                path.chmod(0o755 if path.is_dir() else 0o644)
        subprocess.run(['patch', '-s', '-p1'], input=patch, cwd=temp, check=True)
        for entry in manifest['modules']:
            name = entry['directory']
            subprocess.run(['diff', '-qr', str(temp / name), str(SOURCE / name)], check=True)
            upstream = Path(downloads[name]['Dir'])
            local = SOURCE / name
            files = {p.relative_to(local) for p in local.rglob('*') if p.is_file()}
            original = {p.relative_to(upstream) for p in upstream.rglob('*') if p.is_file() and '.github' not in p.relative_to(upstream).parts}
            changed = sorted(str(f) for f in files | original if
                             not (local / f).exists() or not (upstream / f).exists() or
                             (local / f).read_bytes() != (upstream / f).read_bytes())
            if changed != sorted(entry['changed_files']):
                raise ValueError(f'{name}: changed_files does not describe the source difference')
    print('Native dependency metadata, negative provenance checks and source reconstruction passed.')


if __name__ == '__main__':
    main()
