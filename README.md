<div align = "center">

![Carbon Logo](.github/carbon.png)

</div>

## Installation

```sh
uv add git+https://github.com/iiPythonx/carbon[client]
```

```py
from carbon import CarbonDB
CarbonDB(["localhost"]).write("hello", "world")
```

## Launching

```sh
git clone git@github.com:iiPythonx/carbon
uv venv
uv pip install .[host]
python3 -m carbon
```
