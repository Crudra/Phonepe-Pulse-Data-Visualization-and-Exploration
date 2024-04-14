use phonepe;

CREATE TABLE if not exists aggregated_transaction (states varchar(50),
                                                                      years int,
                                                                      quarter int,
                                                                      transaction_type varchar(50),
                                                                      transaction_count bigint,
                                                                      transaction_amount bigint);
                                                                      
CREATE TABLE if not exists aggregated_user (states varchar(50),
                                                                years int,
                                                                quarter int,
                                                                brands varchar(50),
                                                                transaction_count bigint,
                                                                percentage float);
                                                                
CREATE TABLE if not exists map_transaction (states varchar(50),
                                                                years int,
                                                                quarter int,
                                                                district varchar(50),
                                                                transaction_count bigint,
                                                                transaction_amount float);
                                                                
CREATE TABLE if not exists map_user (states varchar(50),
                                                        years int,
                                                        quarter int,
                                                        districts varchar(50),
                                                        registeredUser bigint,
                                                        appOpens bigint);
                                                        
CREATE TABLE if not exists top_transaction (states varchar(50),
                                                                years int,
                                                                quarter int,
                                                                pincodes int,
                                                                transaction_count bigint,
                                                                transaction_amount bigint);
                                                                
CREATE TABLE if not exists top_user (states varchar(50),
                                                        years int,
                                                        quarter int,
                                                        pincodes int,
                                                        registeredUser bigint
                                                        );