CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

SELECT * FROM indiv_sample_nyc WHERE cmte_id != transaction_amt;

SELECT cmte_id, name FROM indiv_sample_nyc;

SELECT cmte_id, SUM(transaction_amt)
FROM indiv_sample_nyc
GROUP BY cmte_id;

SELECT cmte_id
FROM indiv_sample_nyc
GROUP BY cmte_id
HAVING SUM(transaction_amt) > 10;
