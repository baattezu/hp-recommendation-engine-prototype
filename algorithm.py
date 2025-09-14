# Creating an informative demonstration: compute signals and benefit estimates for a single client (Айгерим)
# using the transaction snippet provided by the user. We'll:
# - parse the CSV snippet
# - compute signal features (savings_ratio, travel_ratio, premium_ratio, etc.)
# - compute benefit estimates for 10 products using rule-based formulas
# - rank products and produce a CSV output client_code,product,push_notification
# - save CSV to /mnt/data/recommendations.csv for download
# We'll use pandas and simple, explainable formulas (configurable business params).
import pandas as pd
from io import StringIO
import math
import os

csv_data_transactions = """client_code,name,product,status,city,date,category,amount,currency
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 09:10:36,Такси,6424.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 09:15:49,Такси,2643.9,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 13:10:49,Смотрим дома,4716.59,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 15:40:10,Такси,3669.43,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 17:40:30,Играем дома,5095.03,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 08:30:08,Смотрим дома,4043.14,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 08:40:51,Играем дома,5377.36,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 12:10:21,Продукты питания,18848.85,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 11:50:37,Такси,5541.49,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 12:10:35,Продукты питания,12139.34,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:00:10,Едим дома,4610.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:00:58,Кафе и рестораны,5635.01,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:40:05,Кафе и рестораны,5029.07,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 19:00:08,Кафе и рестораны,15408.79,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 19:00:39,Кафе и рестораны,5780.32,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 08:20:04,Такси,3914.26,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 12:10:37,Продукты питания,8455.81,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 11:00:21,Играем дома,6371.07,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 12:10:04,Продукты питания,18377.32,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 09:15:57,Такси,2804.57,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 09:40:39,Смотрим дома,3120.13,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 11:50:51,Смотрим дома,4709.72,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 12:10:42,Продукты питания,16376.04,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 18:30:36,АЗС,11038.0,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 19:00:43,Кафе и рестораны,11841.11,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 09:15:01,Такси,4622.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 09:50:20,Кино,5475.53,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 11:30:08,Играем дома,3421.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 16:30:09,Кино,3663.96,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 17:20:07,Смотрим дома,4658.13,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 18:10:47,Такси,6021.04,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 19:40:03,Едим дома,5473.4,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-11 19:00:49,Кафе и рестораны,4871.81,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 09:15:23,Такси,5978.97,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 11:30:02,Кафе и рестораны,7584.76,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 13:10:51,Смотрим дома,2749.59,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 15:40:21,Кафе и рестораны,8826.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 15:40:36,Кино,3952.33,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 16:50:55,Играем дома,5907.11,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 17:30:34,Играем дома,4640.8,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 21:40:57,Смотрим дома,4246.22,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 08:30:08,Кино,4782.15,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 12:10:04,Продукты питания,16646.3,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 13:50:43,Смотрим дома,4223.9,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 17:00:14,Кино,4520.94,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 18:30:32,АЗС,21431.43,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 09:00:01,Кино,9591.56,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 09:50:32,Играем дома,4675.19,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 10:30:00,Путешествия,39623.64,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 12:10:12,Продукты питания,14803.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 15:50:24,Кино,4286.91,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 19:00:40,Кафе и рестораны,5725.94,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 12:10:58,Продукты питания,16157.1,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 16:50:01,Едим дома,4244.07,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 19:00:47,Кафе и рестораны,10877.42,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 20:30:38,Кино,8183.56,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 10:10:19,Играем дома,6379.4,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 12:10:45,Продукты питания,14858.45,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 15:50:07,Кафе и рестораны,5458.05,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 17:20:02,Кино,6678.0,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 19:10:33,Играем дома,5416.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 21:00:54,Кафе и рестораны,7686.93,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 21:40:44,Кино,6625.9,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-20 19:00:16,Кафе и рестораны,7548.57,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 12:10:45,Продукты питания,15885.65,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 13:20:19,Играем дома,4399.72,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 17:30:11,Кино,6341.25,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 19:40:41,Такси,7780.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 17:10:29,Кафе и рестораны,4178.18,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 19:00:52,Кафе и рестораны,6584.84,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 08:10:39,Такси,6050.63,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 12:10:19,Продукты питания,9627.08,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 16:30:48,Такси,3817.36,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 21:50:56,Играем дома,3309.81,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 12:10:26,Продукты питания,17239.49,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 19:00:26,Кафе и рестораны,3153.41,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 20:50:56,Смотрим дома,3574.59,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 08:30:33,Такси,5026.79,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 12:10:48,Продукты питания,22660.33,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 12:30:16,Едим дома,6525.71,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 13:30:03,Едим дома,7368.92,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 20:10:05,Играем дома,5699.58,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 09:15:40,Такси,3959.31,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 12:10:13,Продукты питания,16151.74,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 12:40:44,Кино,2935.11,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 13:00:38,Такси,8939.55,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 16:40:17,Едим дома,6754.99,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 12:10:40,Продукты питания,8221.22,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 14:30:35,Едим дома,3353.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 14:30:37,Кафе и рестораны,5387.6,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 19:00:34,Кафе и рестораны,9348.91,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 09:20:14,Едим дома,4901.95,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 10:10:58,Такси,3541.45,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 12:10:14,Продукты питания,21961.69,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 18:10:35,Играем дома,3534.68,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 19:00:20,Кафе и рестораны,8814.28,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 19:30:04,Едим дома,6940.88,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-30 09:15:39,Такси,2137.27,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-30 17:50:29,Кафе и рестораны,3203.46,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-30 18:20:10,Смотрим дома,6813.12,KZT
"""

csv_data_transfers = """
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 11:40:16,card_out,out,9359.56,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 13:10:59,card_out,out,17590.68,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 20:23:02,card_out,out,19677.33,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-03 09:50:44,card_out,out,37686.28,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-03 14:17:12,card_out,out,7880.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-03 14:54:58,p2p_out,out,34847.69,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 09:38:36,atm_withdrawal,out,26017.05,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 11:44:11,card_out,out,15835.32,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 12:44:42,card_out,out,34387.7,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 18:20:46,card_out,out,18031.61,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 11:43:55,card_out,out,45609.76,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 13:28:35,card_out,out,10501.98,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 16:06:37,atm_withdrawal,out,63623.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 17:29:05,utilities_out,out,43108.03,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 19:30:13,p2p_out,out,29058.01,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 20:40:40,cashback_in,in,5280.98,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-05 20:47:30,card_out,out,7868.37,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-06 10:49:39,card_out,out,12844.37,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 09:38:11,card_out,out,13801.96,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 10:31:13,card_out,out,17198.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 10:33:50,p2p_out,out,23136.11,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 10:49:58,p2p_out,out,13345.18,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 11:34:15,atm_withdrawal,out,46745.77,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-07 18:38:37,card_out,out,11714.15,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 08:33:36,cashback_in,in,16739.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 13:11:11,card_out,out,9590.15,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 13:25:46,card_out,out,30538.57,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-08 15:50:04,loan_payment_out,out,64102.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 08:58:48,p2p_out,out,31247.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-09 21:42:23,p2p_out,out,39132.45,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 11:25:43,p2p_out,out,15359.74,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-10 16:31:28,card_out,out,21815.96,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-11 09:41:29,atm_withdrawal,out,53722.74,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-11 17:33:30,p2p_out,out,14934.15,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 16:59:37,loan_payment_out,out,53048.87,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-12 20:30:54,card_in,in,5302.73,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-13 09:00:38,card_out,out,14860.6,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-13 13:40:31,card_out,out,77292.54,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-13 18:39:41,card_out,out,12452.4,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 09:02:04,card_out,out,63016.38,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 09:08:01,utilities_out,out,22761.83,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 12:02:39,card_out,out,47981.31,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 12:03:21,p2p_out,out,25749.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-14 14:30:22,utilities_out,out,24101.11,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-15 10:44:02,card_out,out,20821.94,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 08:36:24,cashback_in,in,23419.74,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 09:40:26,p2p_out,out,20724.4,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 13:15:57,card_out,out,5968.18,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-16 18:32:30,salary_in,in,488634.28,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-17 12:30:52,p2p_out,out,21946.77,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-17 13:11:04,card_out,out,11494.94,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-17 14:07:08,card_out,out,35586.3,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-17 18:33:27,card_out,out,25828.82,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 10:49:05,card_out,out,11509.87,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 17:03:14,atm_withdrawal,out,19434.3,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 18:35:50,p2p_out,out,21224.83,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 20:25:38,card_out,out,18795.99,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-18 20:40:30,p2p_out,out,38552.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-19 15:52:15,p2p_out,out,20876.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-19 17:51:39,card_out,out,83915.05,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-19 18:47:24,card_in,in,26514.14,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-19 20:50:00,p2p_out,out,23658.43,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-20 09:27:48,p2p_out,out,10917.65,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-20 12:27:17,card_out,out,16462.49,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-20 12:28:20,card_out,out,28974.46,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 09:14:45,card_out,out,20314.86,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 14:40:26,p2p_out,out,11949.39,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-21 15:45:06,p2p_out,out,43120.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 10:46:20,p2p_out,out,29187.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 14:43:24,card_out,out,35081.38,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 19:05:43,card_out,out,12378.79,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 20:57:59,card_out,out,47020.25,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-22 21:48:23,card_in,in,4584.13,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 09:51:35,card_in,in,8913.18,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 15:59:33,card_out,out,28258.9,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-23 17:29:51,card_out,out,7501.95,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 09:14:51,utilities_out,out,22146.8,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 13:12:32,card_out,out,13805.79,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-24 17:02:17,card_out,out,52541.76,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-25 08:21:12,card_out,out,32189.84,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-26 10:46:30,card_out,out,20000.2,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-26 12:20:26,loan_payment_out,out,58150.71,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-26 13:06:00,card_out,out,17073.01,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-26 13:09:17,card_out,out,15208.89,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-26 14:52:34,atm_withdrawal,out,48187.52,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 10:07:18,card_out,out,13000.35,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 12:38:10,utilities_out,out,45719.44,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 15:53:10,card_out,out,15504.4,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-27 20:19:19,card_out,out,12605.1,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 11:15:30,card_out,out,12281.77,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 12:20:58,card_out,out,13171.09,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 15:59:46,refund_in,in,6998.08,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 16:28:34,card_out,out,23921.99,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 17:59:42,card_out,out,20041.45,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-28 19:43:35,card_out,out,17599.17,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 10:12:49,card_out,out,56741.04,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 14:41:01,card_in,in,9223.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-29 18:20:23,card_out,out,10836.51,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-30 09:14:32,refund_in,in,31330.57,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-30 19:21:18,p2p_out,out,29799.82,KZT
"""

df = pd.read_csv(StringIO(csv_data), parse_dates=['date'])

# Business-configurable params (example values, should be tuned by bank)
params = {
    'travel_cashback_rate': 0.04,
    'premium_base_rate': 0.02,
    'premium_category_bonus': 0.04,  # on premium categories
    'credit_card_rate_top3': 0.10,
    'credit_card_online_rate': 0.10,
    'fx_saving_per_tx': 500.0,  # approximate KZT saved per FX event
    'loan_value_buffer_rate': 0.5,  # how much value a quick cash availability gives relative to shortfall
    'deposit_multicurr_rate_annual': 0.03,
    'deposit_saving_frozen_rate_annual': 0.06,
    'deposit_accumulative_rate_annual': 0.04,
    'investment_expected_annual_return': 0.05,
    'gold_expected_annual_return': 0.02,
    'cashback_cap': 200000.0,  # cap per period for cashback calculation (high)
}

# We'll assume some profile fields not present in CSV
profile = {
    'client_code': 1,
    'name': 'Айгерим',
    'status': 'Зарплатный клиент',
    'city': 'Алматы',
    # avg_monthly_balance_KZT is not provided in snippet; choose a reasonable assumed value for demonstration
    'avg_monthly_balance_KZT': 120_000.0
}

# Helper: categorize sets (in cyrillic as in dataset)
premium_categories = ['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны']
travel_categories = ['Путешествия', 'Отели', 'Такси']
online_categories = ['Едим дома', 'Смотрим дома', 'Играем дома']

# Aggregations
total_spend = df['amount'].sum()
# assume data covers 1 month (June 2025) for this demonstration
months_covered = 1.0
monthly_spend = total_spend / months_covered

# per-category spend
category_spend = df.groupby('category')['amount'].sum().to_dict()

def sum_categories(cat_list):
    return sum(category_spend.get(c, 0.0) for c in cat_list)

travel_spend = sum_categories(travel_categories)
premium_spend = sum_categories(premium_categories)
online_spend = sum_categories(online_categories)

# Signals
signals = {}
signals['total_spend'] = total_spend
signals['monthly_spend'] = monthly_spend
signals['avg_monthly_balance'] = profile['avg_monthly_balance_KZT']

# savings_ratio = avg_balance / (avg_balance + monthly_spend) -> 0..1 (higher means more saved)
signals['savings_ratio'] = profile['avg_monthly_balance_KZT'] / (profile['avg_monthly_balance_KZT'] + max(monthly_spend,1))
# spending_stability cannot be estimated reliably from 1 month -> use default medium
signals['spending_stability'] = 0.5
signals['travel_ratio'] = travel_spend / total_spend if total_spend>0 else 0.0
signals['premium_ratio'] = premium_spend / total_spend if total_spend>0 else 0.0
signals['online_ratio'] = online_spend / total_spend if total_spend>0 else 0.0
signals['fx_activity'] = 0  # no fx records in provided snippet
signals['investment_activity'] = 0
signals['credit_utilization'] = 0  # no credit info provided

# Determine top categories
top_categories = sorted(category_spend.items(), key=lambda x: x[1], reverse=True)
top3 = [c for c,_ in top_categories[:3]]

# Benefit calculations (rule-based, transparent)
benefits = {}

# 1. Карта для путешествий
benefits['Карта для путешествий'] = min(params['travel_cashback_rate'] * travel_spend, params['cashback_cap'])

# 2. Премиальная карта
base_benefit_premium = params['premium_base_rate'] * total_spend
category_bonus = params['premium_category_bonus'] * premium_spend
saved_fees = 0.0  # need fees data to estimate; set 0 in demo
benefits['Премиальная карта'] = min(base_benefit_premium + category_bonus + saved_fees, params['cashback_cap'])

# 3. Кредитная карта
top3_spend = sum(category_spend.get(c,0) for c in top3)
credit_card_benefit = params['credit_card_rate_top3'] * top3_spend + params['credit_card_online_rate'] * online_spend
benefits['Кредитная карта'] = min(credit_card_benefit, params['cashback_cap'])

# 4. Обмен валют
benefits['Обмен валют'] = params['fx_saving_per_tx'] * signals['fx_activity']

# 5. Кредит наличными
# If client has low savings_ratio relative to threshold, value of fast liquidity = shortfall * buffer_rate
monthly_shortfall = max(0.0, monthly_spend - profile['avg_monthly_balance_KZT'])
benefits['Кредит наличными'] = params['loan_value_buffer_rate'] * monthly_shortfall

# 6. Депозит мультивалютный
free_balance = max(0.0, profile['avg_monthly_balance_KZT'] - 0.1*profile['avg_monthly_balance_KZT'])  # assume 10% locked buffer
benefits['Депозит мультивалютный'] = free_balance * (params['deposit_multicurr_rate_annual'] / 12.0)  # monthly interest approx

# 7. Депозит сберегательный (заморозка)
# Valuable if savings_ratio high and stability high
if signals['savings_ratio'] > 0.5 and signals['spending_stability'] > 0.6:
    benefits['Депозит сберегательный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_saving_frozen_rate_annual']/12.0)
else:
    benefits['Депозит сберегательный'] = 0.0

# 8. Депозит накопительный
# Good for people with regular small top-ups; we don't have that signal -> small default
benefits['Депозит накопительный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_accumulative_rate_annual']/12.0) * 0.4

# 9. Инвестиции (брокерский счёт)
# Value = expected return on free balance (conservative, not guaranteed)
benefits['Инвестиции (брокерский счёт)'] = free_balance * (params['investment_expected_annual_return']/12.0) * 0.6

# 10. Золотые слитки
benefits['Золотые слитки'] = free_balance * (params['gold_expected_annual_return']/12.0) * 0.3

# Build DataFrame for signals and benefits
signals_df = pd.DataFrame([signals]).T.reset_index()
signals_df.columns = ['signal','value']

benefits_df = pd.DataFrame(list(benefits.items()), columns=['product','benefit_KZT']).sort_values('benefit_KZT', ascending=False).reset_index(drop=True)

# Rank and choose top product
best_product = benefits_df.iloc[0]['product']
best_value = benefits_df.iloc[0]['benefit_KZT']

# Format amount with space thousand separator and comma decimal (e.g., 27 400,50 ₸)
def format_amount_kzt(x):
    # round to nearest Tenge for clarity
    x = float(x)
    rounded = round(x, 2)
    whole = int(math.floor(rounded))
    frac = int(round((rounded - whole) * 100))
    whole_with_sep = f"{whole:,}".replace(",", " ")
    if frac == 0:
        return f"{whole_with_sep} ₸"
    else:
        return f"{whole_with_sep},{frac:02d} ₸"

# Build push notification for best product (use templates)
templates = {
    'Карта для путешествий': "{name}, в июне у вас траты на такси {taxi_sum}. С тревел-картой часть расходов вернулась бы кешбэком ≈{benefit}. Открыть карту.",
    'Премиальная карта': "{name}, у вас стабильно крупный остаток и траты в ресторанах {rest_sum}. Премиальная карта даст повышенный кешбэк и бесплатные снятия. Подключите сейчас.",
    'Кредитная карта': "{name}, ваши топ-категории — {cat1}, {cat2}, {cat3}. Кредитная карта даёт до 10% в любимых категориях. Оформить карту.",
    'Обмен валют': "{name}, вы часто платите в валюте. В приложении выгодный обмен и авто-покупка по целевому курсу. Настроить обмен.",
    'Кредит наличными': "{name}, если нужен запас на крупные траты — можно оформить кредит наличными с гибкими выплатами. Узнать доступный лимит.",
    'Депозит мультивалютный': "{name}, у вас остаются свободные средства. Разместите их на мультивалютном вкладе — удобно хранить валюту и получать процент. Открыть вклад.",
    'Депозит сберегательный': "{name}, у вас стабильный остаток. Разместите средства на сберегательном вкладе с повышенной ставкой. Открыть вклад.",
    'Депозит накопительный': "{name}, хотите копить с удобными пополнениями? Накопительный вклад поможет. Открыть вклад.",
    'Инвестиции (брокерский счёт)': "{name}, попробуйте инвестиции с низким порогом входа и без комиссий на старт. Открыть счёт.",
    'Золотые слитки': "{name}, защитите сбережения — золото может помочь в диверсификации. Посмотреть варианты."
}

taxi_sum = sum(c for k,c in category_spend.items() if 'Такси' in k)
rest_sum = category_spend.get('Кафе и рестораны', 0.0)

push_text = templates.get(best_product, "{name}, у нас есть предложение для вас. Посмотреть.")\
    .format(name=profile['name'],
            taxi_sum=format_amount_kzt(taxi_sum),
            benefit=format_amount_kzt(best_value),
            rest_sum=format_amount_kzt(rest_sum),
            cat1=top3[0] if len(top3)>0 else '',
            cat2=top3[1] if len(top3)>1 else '',
            cat3=top3[2] if len(top3)>2 else '')

# Enforce red-policy checks: length 180-220 chars preferred for push; but we'll keep concise for demo
def red_policy_ok(text):
    # checks: no ALL CAPS, <=1 exclamation, no forbidden words (simple)
    if text.upper() == text and any(c.isalpha() for c in text):  # all caps
        return False, "ALL_CAPS"
    if text.count('!') > 1:
        return False, "TOO_MANY_EXCLAMATIONS"
    if len(text) > 220:
        return False, "TOO_LONG"
    return True, ""

rp_ok, rp_reason = red_policy_ok(push_text)

# Prepare CSV output
output_df = pd.DataFrame([{
    'client_code': profile['client_code'],
    'product': best_product,
    'push_notification': push_text
}])

out_path = "mnt/data/recommendations.csv"
output_df.to_csv(out_path, index=False)

# Display results: signals, benefits, ranked products, selected push, and save CSV.
signals_df_display = signals_df.copy()
benefits_df_display = benefits_df.copy()
output_preview = output_df.copy()

signals_df_display['value'] = signals_df_display['value'].apply(lambda x: round(x,4) if isinstance(x,(float,int)) else x)

# Show some explanatory notes
notes = {
    'assumptions': [
        "avg_monthly_balance_KZT was assumed = 120 000 ₸ (not provided in snippet).",
        "Data covers a 1-month window (June 2025) in the snippet; some signals (stability) are set to defaults where insufficient history exists.",
        "Benefit formulas are rule-based, transparent, and configurable via params dict."
    ],
    'red_policy': {"ok": rp_ok, "reason": rp_reason},
    'best_product': best_product,
    'best_value_KZT': best_value
}

# Save a more detailed CSV with all benefit estimates per product for the client
detailed_out = benefits_df_display.copy()
detailed_out['benefit_KZT_formatted'] = detailed_out['benefit_KZT'].apply(format_amount_kzt)
detailed_csv_path = "mnt/data/recommendations_detailed.csv"
detailed_out.to_csv(detailed_csv_path, index=False)



# Print summary
print("Best product:", best_product)
print("Estimated benefit:", format_amount_kzt(best_value))
print("Push preview:", push_text)
print("\nFiles saved:")
print(" - Recommendations (simple):", out_path)
print(" - Recommendations (detailed):", detailed_csv_path)

# Return some python-visible objects (for notebook output)
{
    "signals": signals,
    "benefits_sorted": benefits_df.to_dict('records'),
    "push_text": push_text,
    "csv_path": out_path,
    "detailed_csv_path": detailed_csv_path,
    "notes": notes
}
