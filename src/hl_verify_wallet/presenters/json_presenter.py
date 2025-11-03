from ..ports.presenter import Presenter
from typing import Any, Dict
import json

class JsonPresenter(Presenter):
    def render(self, result: Dict[str, Any]) -> None:
        print(json.dumps(result, default=str, ensure_ascii=False))
