import random
import pandas as pd
import numpy as np
import collections
import csv 
import os


# generate random payment type
def random_payment_type():
    payment_type = ["Card", "Internet Banking", "UPI", "Wallet"]
    result_payment_type = random.choice(payment_type)
    return result_payment_type

# generate random payment transaction confirmation id
def payment_txn_id():
    txn_id = random.randint(10000, 99999)
    return txn_id

# generate random payment success or failure
def payment_txn_success():
    txn_success = random.choice(["Y", "N"])
    result_reason_type = None
    if txn_success == "N":
        reason_type = ["Invalid Card", "Invalid Internet Banking Account", "Invalid UPI Account", "Invalid CVV"]
        result_reason_type = random.choice(reason_type)
    return txn_success, result_reason_type

def generate_payments(num_records):
    payment_data = []
    for _ in range(num_records):
        payment_type = random_payment_type()
        txn_id = payment_txn_id()
        txn_success, failure_reason = payment_txn_success()
        payment_data.append({
            "payment_type": payment_type,
            "payment_txn_id": txn_id,
            "payment_txn_success": txn_success,
            "failure_reason": failure_reason if txn_success == "N" else " "
        })
    return payment_data

def write_to_csv(payment_data, filename):
    with open(filename, mode = 'w', newline = '', encoding = 'utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["payment_type", "payment_txn_id", "payment_txn_success", "failure_reason"])
        writer.writeheader()
        writer.writerows(payment_data)

if __name__ == "__main__":
    num_records = random.randint(10000,15000)
    payment_data = generate_payments(num_records)
    write_to_csv(payment_data, "transactions.csv")
    print(f"Generated {num_records} payment records and saved to 'transactions.csv'")


