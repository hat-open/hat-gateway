from pathlib import Path
import subprocess
import sys

from hat import json
from hat.doit import common


__all__ = ['task_clean_all',
           'task_build',
           'task_check',
           'task_test',
           'task_docs',
           'task_json_schema_repo']


build_dir = Path('build')
src_py_dir = Path('src_py')
pytest_dir = Path('test_pytest')
docs_dir = Path('docs')
schemas_json_dir = Path('schemas_json')

build_py_dir = build_dir / 'py'
build_docs_dir = build_dir / 'docs'

json_schema_repo_path = src_py_dir / 'hat/gateway/json_schema_repo.json'


def task_clean_all():
    """Clean all"""
    return {'actions': [(common.rm_rf, [build_dir,
                                        json_schema_repo_path])]}


def task_build():
    """Build"""

    def build():
        common.wheel_build(
            src_dir=src_py_dir,
            dst_dir=build_py_dir,
            src_paths=list(common.path_rglob(src_py_dir,
                                             blacklist={'__pycache__'})),
            name='hat-gateway',
            description='Hat gateway',
            url='https://github.com/hat-open/hat-gateway',
            license=common.License.APACHE2,
            packages=['hat'],
            console_scripts=['hat-gateway = hat.gateway.main:main'])

    return {'actions': [build],
            'task_dep': ['json_schema_repo']}


def task_check():
    """Check with flake8"""
    return {'actions': [(_run_flake8, [src_py_dir]),
                        (_run_flake8, [pytest_dir])]}


def task_test():
    """Test"""

    def run(args):
        subprocess.run([sys.executable, '-m', 'pytest',
                        '-s', '-p', 'no:cacheprovider',
                        *(args or [])],
                       cwd=str(pytest_dir),
                       check=True)

    return {'actions': [run],
            'pos_arg': 'args',
            'task_dep': ['json_schema_repo']}


def task_docs():
    """Docs"""

    def build():
        common.sphinx_build(common.SphinxOutputType.HTML, docs_dir,
                            build_docs_dir)
        subprocess.run([sys.executable, '-m', 'pdoc',
                        '--html', '--skip-errors', '-f',
                        '-o', str(build_docs_dir / 'py_api'),
                        'hat.gateway'],
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL,
                       check=True)

    return {'actions': [build],
            'task_dep': ['json_schema_repo']}


def task_json_schema_repo():
    """Generate JSON Schema Repository"""
    src_paths = list(schemas_json_dir.rglob('*.yaml'))

    def generate():
        repo = json.SchemaRepository(*src_paths)
        data = repo.to_json()
        json.encode_file(data, json_schema_repo_path, indent=None)

    return {'actions': [generate],
            'file_dep': src_paths,
            'targets': [json_schema_repo_path]}


def _run_flake8(path):
    subprocess.run([sys.executable, '-m', 'flake8', str(path)],
                   check=True)
