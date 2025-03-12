package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public KafkaConsumer(UserRepository userRepository, TransactionRepository transactionRepository,RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(ConsumerRecord<String, Transaction> record) {
        Transaction transaction = record.value();
        System.out.println("Received Transaction: " + transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
 
            if (sender.getBalance() >= transaction.getAmount()) {
                sender.setBalance(sender.getBalance() - transaction.getAmount());


                // Get incentive amount from API
                String incentiveApiUrl = "http://localhost:8080/incentive";
                Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
                
                float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

                // Add to recipient (transaction amount + incentive)
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);


                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRepository.save(transactionRecord);

                System.out.println("Transaction processed successfully!");
                System.out.println("Transaction processed successfully! Incentive applied: " + incentiveAmount);

            } else {
                System.out.println("Transaction rejected: Insufficient funds.");
            }
        } else {
            System.out.println("Transaction rejected: Invalid sender or recipient.");
        }
    }
}
