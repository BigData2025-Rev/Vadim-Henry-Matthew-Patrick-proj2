import random


class PaymentGenerator:
    # generate random payment type, where 90% of payment types is wallet and card
    def random_payment_type(self):
        payment_type = ["Card", "Internet Banking", "UPI", "Wallet"]
        result_payment_type = random.choices(payment_type, weights = (45,5,5,45), k=1)[0]
        return result_payment_type

    # generate random payment transaction confirmation id
    def payment_txn_id(self):
        txn_id = random.randint(1000000000, 9999999999)
        return txn_id

    # generate random payment success or failure with 90% of transactions are successful
    def get_payment_txn_list(self,num_records):
        success_rate = ["Y", "N"]
        txn_success_list = random.choices(success_rate, weights = (9,1), k=num_records)
        return txn_success_list

    # generate a reason for transaction failure
    def get_failure_reason(self):
        reason_type = ["Invalid Card", "Invalid Internet Banking Account", "Invalid UPI Account", "Invalid CVV"]
        failure_reason = random.choice(reason_type) 
        return failure_reason      

    # generate list of payment records
    def generate_payments(self,num_records):
        payment_data = []
        txn_success_list = self.get_payment_txn_list(num_records)
        for idx in range(num_records):
            payment_type = self.random_payment_type()
            txn_id = self.payment_txn_id()
            txn_success = txn_success_list[idx]
            failure_reason = ""
            if txn_success == "N":
                failure_reason = self.get_failure_reason()
            payment_data.append({
                "payment_type": payment_type,
                "payment_txn_id": txn_id,
                "payment_txn_success": txn_success,
                "failure_reason": failure_reason if txn_success == "N" else " "
            })
        return payment_data



