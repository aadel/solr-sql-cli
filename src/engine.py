class Engine:

    sa_engine = None

    @staticmethod
    def set_engine(engine):
        Engine.sa_engine = engine

    @staticmethod
    def get_engine():
        return Engine.sa_engine
