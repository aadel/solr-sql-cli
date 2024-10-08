import csv
import io

import tableprint as tp
from output_format import OutputFormat
from sqlalchemy.engine import LegacyCursorResult


class OutputWriter:

    def __init__(self, output_format: OutputFormat, out: io.TextIOWrapper):
        self.output_format = output_format
        self.out = out

    def write(self, result: LegacyCursorResult) -> str:
        rows = []

        for row in result:
            data = []
            for v in row._data:
                if v is None:
                    data.append("")
                else:
                    data.append(v)
            rows.append(data)

        if self.output_format == OutputFormat.TABULAR:
            tp.table(rows, result.keys(), align="left", out=self.out)
        elif self.output_format == OutputFormat.CSV:
            header = result.keys()
            csvwriter = csv.writer(self.out)

            csvwriter.writerow(header)
            for row in rows:
                csvwriter.writerow(row)
