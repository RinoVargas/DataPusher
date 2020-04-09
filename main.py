from config.configurer import ConfigurationLoader
from config.engine import DBEngineConfigurator
from config.push import DataPusher


def main():
    config = ConfigurationLoader()
    engine = DBEngineConfigurator(config)
    pusher = DataPusher(config, engine)
    pusher.push()


if __name__ == '__main__':
    main()