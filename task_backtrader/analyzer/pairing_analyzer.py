import backtrader as bt
from loguru import logger
from datetime import datetime
import itertools
import math

class PairingAnalyzer(bt.Analyzer):
    """
    Pairing Analyzer calculates R-values between pairs of products.
    R-value is defined as the ratio of net values between two products.
    """
    
    def __init__(self):
        # Generate all possible pairs of data feeds
        self.pairs = list(itertools.combinations(range(len(self.datas)), 2))
        self.pairing_data = []
        self.pairing_data_rvalues = {}
        self.pairing_data_dicts = {}
        
    def start(self):
        self.pairing_data = []
        
    def next(self):
        current_date = self.strategy.datetime.date()
        pair_values = {}
        daily_data = {
            'date': current_date
        }
        # Calculate R-values for all pairs
        for pair in self.pairs:
            data1 = self.datas[pair[0]]
            data2 = self.datas[pair[1]]

            if data1.datetime.date(0) != current_date or data2.datetime.date(0) != current_date:
                continue
            
            # Get product values
            value1 = data1.close[0]
            value2 = data2.close[0]
            

            if value1 == 0 or value2 == 0:
                continue
            
            # Handle None or NaN values
            if value1 is None or math.isnan(value1) or value1 == 0:
                continue
            if value2 is None or math.isnan(value2) or value2 == 0:
                continue

            # Calculate R-value (avoid division by zero)
            r_value = value1 / value2 if value2 != 0 else float('inf')
            
            pair_key = f"{data1._name}/{data2._name}"
            daily_data[pair_key] = r_value
            if pair_key not in self.pairing_data_rvalues:
                self.pairing_data_rvalues[pair_key] = []
            self.pairing_data_rvalues[pair_key].append(float(r_value))
            # Calculate standard deviation for last 200 values if enough data points
            std = 0
            avg = 0
            if len(self.pairing_data_rvalues[pair_key]) > 200:
                last_200_values = self.pairing_data_rvalues[pair_key][-200:]
                # Calculate mean
                avg = sum(last_200_values) / len(last_200_values)
                # Calculate standard deviation
                squared_diff_sum = sum((x - avg) ** 2 for x in last_200_values)
                std = math.sqrt(squared_diff_sum / len(last_200_values))

                daily_data[f"{pair_key}_std"] = std
                daily_data[f"{pair_key}_avg"] = avg
                daily_data[f"{pair_key}_r_upper"] = avg + 1.5 * std
                daily_data[f"{pair_key}_r_lower"] = avg - 1.5 * std
            else:
                daily_data[f"{pair_key}_std"] = std
                daily_data[f"{pair_key}_avg"] = avg
                daily_data[f"{pair_key}_r_upper"] = 0
                daily_data[f"{pair_key}_r_lower"] = 0
            if pair_key not in self.pairing_data_dicts:
                self.pairing_data_dicts[pair_key] = []
            self.pairing_data_dicts[pair_key].append({
                "date": current_date,
                "r_value": float(r_value),
                "std": std,
                "avg": avg,
                "r_upper": avg + 1.5 * std,
                "r_lower": avg - 1.5 * std
            })

        if (self.strategy.is_open_traded and not self.strategy.is_close_traded):
            self.pairing_data.append(daily_data)

    def stop(self):
        pass
        
    def get_analysis(self):
        """Return the pairing analysis data"""
        return self.pairing_data 

    def get_pairing_data_dict(self, pair_key):
        return self.pairing_data_dicts[pair_key]