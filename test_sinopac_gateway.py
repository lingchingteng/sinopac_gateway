import time
from logging import INFO

from dotenv import dotenv_values
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.setting import SETTINGS

from sinopac_gateway import SinopacGateway


def test_connect():
    SETTINGS["log.active"] = True
    SETTINGS["log.level"] = INFO
    SETTINGS["log.console"] = True

    SETTINGS["log.file"] = True

    sinopac_setting = dotenv_values(".env")

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.write_log("主引擎创建成功")

    main_engine.add_gateway(SinopacGateway)
    main_engine.connect(sinopac_setting, "Sinopac")

    while len(main_engine.get_gateway("Sinopac").loaded_contract_lists) != 4:
        main_engine.write_log("Wait all contract list downloaded...")
        time.sleep(1)

    main_engine.close()


if __name__ == "__main__":
    test_connect()
