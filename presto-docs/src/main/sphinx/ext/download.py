#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# noinspection PyUnresolvedReferences
from docutils import nodes, utils
# noinspection PyUnresolvedReferences
from sphinx.errors import SphinxError


TD_LINK_SLIDER = 'http://archive.apache.org/dist/incubator/slider/0.80.0-incubating/slider-assembly-0.80.0-incubating-all.tar.gz'
TD_LINK_PRESTO = 'http://www.teradata.com/presto'

GROUP_ID = 'com.facebook.presto'
ARTIFACTS = {
    'demo-cdh': ('presto-0.141t-demo-cdh', 'ova', TD_LINK_PRESTO),
    'demo-hdp': ('presto-0.141t-demo-cdh', 'ova', TD_LINK_PRESTO),

    'presto-server-pkg': ('presto_server_pkg.141t', 'tar.gz', TD_LINK_PRESTO),
    'presto-ambari-pkg': ('presto_ambari_pkg.141t', 'tar.gz', TD_LINK_PRESTO),
    'presto-client-pkg': ('presto_client_pkg.141t', 'tar.gz', TD_LINK_PRESTO),
    'jdbc': ('presto_client_pkg.141', 'tar.gz', TD_LINK_PRESTO),

    'odbc-documentation': ('Teradata ODBC Driver for Presto Install Guide', 'pdf', TD_LINK_PRESTO),
    'presto-docs': ('presto-docs-0.141t-download', 'zip', TD_LINK_PRESTO),

    'apache-slider': ('slider-assembly-0.80.0-incubating-all', 'tar.gz', TD_LINK_PRESTO)
}

def setup(app):
    # noinspection PyDefaultArgument,PyUnusedLocal
    def download_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text)
            return [], []

        artifact, packaging, uri = ARTIFACTS[text]

        title = artifact + '.' + packaging
        uri = uri

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []

    app.add_role('download', download_link_role)
