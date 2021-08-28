from datetime import datetime
from os import write
import time
from logging import INFO

from vnpy.trader.constant import Interval, Product
from dotenv import dotenv_values
from vnpy.app.data_manager import DataManagerApp
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.setting import SETTINGS

from sinopac_gateway import SinopacGateway


def daily_update():
    SETTINGS["log.active"] = True
    SETTINGS["log.level"] = INFO
    SETTINGS["log.console"] = True

    SETTINGS["log.file"] = True

    sinopac_setting = dotenv_values(".env")

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.write_log("主引擎创建成功")

    main_engine.add_app(DataManagerApp)

    main_engine.add_gateway(SinopacGateway)
    main_engine.connect(sinopac_setting, "Sinopac")

    while len(main_engine.get_gateway("Sinopac").loaded_contract_lists) != 4:
        main_engine.write_log("Wait all contract list downloaded...")
        time.sleep(1)

    all_contracts = main_engine.get_engine("oms").get_all_contracts()

    main_engine.write_log(f"Total {len(all_contracts)} contracts")

    data_manager = main_engine.get_engine("DataManager")

    bar_overview = data_manager.get_bar_overview()

    last_bar_dict = {}

    for b in bar_overview:
        last_bar_dict[(b.symbol, b.exchange, b.interval)] = b.end

    download_cnt = 0
    for contract in all_contracts:
        if contract.product not in [
            Product.EQUITY,
            Product.FUTURES,
            Product.OPTION,
            Product.INDEX,
        ]:
            continue

        start_datetime = last_bar_dict.get(
            (contract.symbol, contract.exchange, Interval.MINUTE),
            datetime(year=2018, month=8, day=18),
        )
        main_engine.write_log(
            f"{download_cnt:4d}: Download {contract.vt_symbol} from {start_datetime}"
        )
        try:
            start_time = time.time()
            num_data = data_manager.download_bar_data(
                symbol=contract.symbol,
                exchange=contract.exchange,
                interval=Interval.MINUTE,
                start=start_datetime,
            )
            main_engine.write_log(
                f"Download {contract.vt_symbol} {num_data} {Interval.MINUTE.value} kbars cost {time.time() - start_time} seconds"
            )
        except Exception as e:
            main_engine.write_log(str(e))

        download_cnt += 1

    main_engine.write_log("Done!")

    main_engine.close()


if __name__ == "__main__":
    daily_update()
