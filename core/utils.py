# core/utils.py

import time
from functools import wraps
from selenium.common.exceptions import WebDriverException

def retry(tentativas=3, delay=5):
    def decorator_retry(func):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            tentativas_restantes = tentativas
            while tentativas_restantes > 0:
                try:
                    return func(*args, **kwargs)
                except WebDriverException as e:
                    tentativas_restantes -= 1
                    error_msg = str(e).splitlines()[0] if str(e) else "Erro desconhecido no WebDriver"
                    print(f"AVISO: Ação falhou. Tentando novamente em {delay}s... ({tentativas_restantes} restantes). Erro: {error_msg}")
                    if tentativas_restantes == 0:
                        print(f"ERRO CRÍTICO: Ação falhou após {tentativas} tentativas.")
                        raise
                    time.sleep(delay)
        return wrapper_retry
    return decorator_retry