from pyspark.sql import SparkSession


class InteractiveBroadcastJoin:

    def __init__(self):
        self._spark = SparkSession.builder.getOrCreate()

    def join(self, large_df, small_df, keys, how='inner', num_interaction=1):

        if how == 'inner':

            _df = self._inner_join(large_df, small_df, keys, num_interaction)

        elif how == 'left':

            _df = self._left_join(large_df, small_df, keys, num_interaction)

        else:
            raise NotImplementedError("how must be 'inner' or 'left'")

        return _df

    def _inner_join(self, large_df, small_df, keys, num_interactions):
        raise NotImplementedError("Under Construction")

    def _left_join(self, large_df, small_df, keys, num_interactions):
        raise NotImplementedError("Under Construction")


def _test():
    pass


if __name__ == '__main__':
    _test()
