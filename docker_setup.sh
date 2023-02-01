#!/bin/bash
(pyenv activate aws-cdk && cd tahoe_infrastructure && pyenv exec python -m venv .venv && pyenv deactivate aws-cdk)
(cd tahoe_infrastructure && source .venv/bin/activate && pip3 install -r requirements.txt)
(cd bin && chmod 777 build_common_whls.sh && ls -l)
