
([ -d "../base/common/dist" ] && cd ../base/common && rm -rf dist)
(cd ../base/common && python3 setup-common.py bdist_wheel)
(cd ../base/common && python3 setup-logger.py bdist_wheel)
(cd ../tahoe-cli && python3 -m pip install .)
cd ../base/common/dist 
for f in *.whl; do pip install $f --force-reinstall; done
for f in *.whl; do pip install $f --target=../lambda/python/lib/python3.9/site-packages --force-reinstall --upgrade; done
