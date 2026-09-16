"""離線測試用 stub：本機無法安裝 line-bot-sdk / gspread，只替換 import，不改 app.py。"""
import sys, types
class _Any:
    def __init__(self,*a,**k): pass
    def __call__(self,*a,**k): return _Any()
    def __getattr__(self,n): return _Any()
    def add(self,*a,**k): return lambda f: f
def _mod(name, attrs=()):
    m = types.ModuleType(name)
    for a in attrs: setattr(m, a, _Any)
    m.__getattr__ = lambda n: _Any
    sys.modules[name] = m
    return m
for n in ["linebot","linebot.v3","linebot.v3.exceptions","linebot.v3.messaging","linebot.v3.webhooks",
          "gspread","google","google.oauth2","google.oauth2.service_account"]:
    _mod(n)
sys.modules["linebot.v3"].WebhookHandler = type("WH",(),{"__init__":lambda s,*a:None,"add":lambda s,*a,**k:(lambda f:f),"handle":lambda s,*a:None})
sys.modules["linebot.v3.exceptions"].InvalidSignatureError = type("ISE",(Exception,),{})
import importlib.util, os
os.environ.setdefault("LINE_CHANNEL_SECRET","x")
def load_app():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, here)
    spec = importlib.util.spec_from_file_location("app", os.path.join(here,"app.py"))
    app = importlib.util.module_from_spec(spec); spec.loader.exec_module(app)
    return app
