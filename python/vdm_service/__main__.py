"""Start with python -m vdm_service (one process per data directory)."""
import logging
import uvicorn
from .app import create_app
from .config import Settings
from .runtime import Service


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
    settings = Settings.load()
    app = create_app(Service(settings))
    uvicorn.run(app,host=settings.host,port=settings.port,workers=1)

if __name__ == '__main__':
    main()
