from pathlib import Path

import hat.doit.sphinx


root_path = Path(__file__).parent.parent.resolve()
static_path = Path(hat.doit.sphinx.__file__).parent / 'static'

extensions = [
    'sphinx.ext.todo',
    'sphinx.ext.graphviz',
    'sphinxcontrib.plantuml',
    'sphinxcontrib.programoutput',
]

version = (root_path / 'VERSION').read_text().strip()
project = 'hat-gateway'
copyright = '2020-2022, Hat Open AUTHORS'
master_doc = 'index'

html_theme = 'furo'
html_static_path = [str(static_path)]
html_css_files = ['hat.css']
html_use_index = False
html_show_sourcelink = False
html_show_sphinx = False
html_sidebars = {'**': ["sidebar/brand.html",
                        "sidebar/scroll-start.html",
                        "sidebar/navigation.html",
                        "sidebar/scroll-end.html"]}

todo_include_todos = True
