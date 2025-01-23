# Databricks notebook source
dbutils.widgets.text("catalog_use", "main")
dbutils.widgets.text("schema_use", "hm_dday")

# COMMAND ----------

catalog_use = dbutils.widgets.get("catalog_use")
schema_use = dbutils.widgets.get("schema_use")

# COMMAND ----------

spark.sql(f"create catalog if not exists {catalog_use}")
spark.sql(f"use catalog {catalog_use}")
display(
  spark.sql("select current_catalog()")
)

# COMMAND ----------

spark.sql(f"create schema if not exists {schema_use}")
spark.sql(f"use schema {schema_use}")
display(
  spark.sql("select current_catalog(), current_schema()")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing;
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC Uncomment the below code chunck for running with a proxy.  

# COMMAND ----------

# %sh

# export https_proxy=inetgate.highmark.com:9121

# COMMAND ----------

import os

os.environ['VOLUME_PATH'] = f"/Volumes/{catalog_use}/{schema_use}/landing/"

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC echo $VOLUME_PATH;

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cd $VOLUME_PATH;
# MAGIC
# MAGIC echo "Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json
# MAGIC Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json
# MAGIC Abraham_Wiza_1f9211d6-4232-4e9e-0e9b-37b18575e22f.json
# MAGIC Abram_Smith_46b02f76-347b-a225-8baf-035f7fbbd276.json
# MAGIC Adah_Stehr_40032a3a-bd84-f8da-1bda-8395edf7bd54.json
# MAGIC Adam_Prohaska_7784a7cb-cab3-3bff-98a0-548b35dba638.json
# MAGIC Adela_Narváez_faef7ddf-dee8-0e10-077d-06670b812a86.json
# MAGIC Adina_Parker_f2728ce0-dfd1-db6c-03cc-82c2ef1c48f7.json
# MAGIC Adrienne_Blanda_bca6d51d-d60d-4426-aa49-3080aaeae9e8.json
# MAGIC Adán_Contreras_6cbdcb86-57d6-a969-f7e2-6c37de9d61f8.json
# MAGIC Adán_Cuellar_cc5aa413-353e-b05d-f650-181859dd8a98.json
# MAGIC Agustin_Bashirian_7ce9f8b4-0523-4de3-b12a-afa842ea7690.json
# MAGIC Agustina_Heller_36dea1da-8de2-9051-faf9-b58746e7cd72.json
# MAGIC Alan_Bednar_3c9ba334-71a8-7dde-8955-119e2481a134.json
# MAGIC Alan_Kessler_322e03f1-bb87-aa8b-b143-735d22f79163.json
# MAGIC Alan_Runte_45dfb9d3-c697-47c2-a08e-7a83298cc688.json
# MAGIC Alba_Harber_727e95f1-a95e-ff9a-d304-0b30f710c60f.json
# MAGIC Alba_Will_b2029f9c-de5d-1f8e-45c7-56123dcb3f40.json
# MAGIC Albert_Leannon_e12f5f22-dc09-d031-8b85-fed94fc0c0fe.json
# MAGIC Albert_Wiza_3d315a3c-479c-80a7-406a-bf9f78af9156.json
# MAGIC Aldo_Gulgowski_0e80a9dc-697a-80fa-9498-20418bdc0bbb.json
# MAGIC Aldo_Hyatt_e7c95ee6-033b-4ecf-c75a-aa1432234c5f.json
# MAGIC Aldo_Lynch_931b82ca-e0ae-9d5c-0f85-a84e26562bca.json
# MAGIC Aleen_Konopelski_f5d9e509-1c8a-e0a1-bf5b-5be3a6dfef41.json
# MAGIC Alene_Predovic_dbf6ed37-d403-909e-d357-b106d3a4c9fb.json
# MAGIC Alene_Smitham_bf46e8c2-f47a-934d-718f-f7bff0d1f56a.json
# MAGIC Alethia_Gorczany_78c44193-2c63-e946-3804-3fad9f9d9970.json
# MAGIC Alexis_Kassulke_125e85ba-e50f-531e-ffa1-84ffe452f92f.json
# MAGIC Alfonso_Hamill_84085219-9ec5-284b-34eb-840fc5863111.json
# MAGIC Alfreda_Prohaska_5566819c-d870-e058-3853-126e3fdda52a.json
# MAGIC Ali_Bailey_c06cda7b-8500-5377-eae2-5affbd8fb581.json
# MAGIC Ali_Hodkiewicz_0500c0be-f152-e194-09f1-03487614b6f0.json
# MAGIC Alica_Batz_db2baba1-0b12-aeb2-afc4-dcde6656df32.json
# MAGIC Aline_Mitchell_c5f9f39d-6a20-4836-1038-bace88486f11.json
# MAGIC Aline_O'Kon_31b6d2f0-8e56-36b6-447b-f9f1dba7bcbe.json
# MAGIC Alita_Crona_1a5e6090-dc84-4f89-5264-cee11ac9a994.json
# MAGIC Alla_Ratke_46cdf285-30a6-6cab-e098-af8f2ae40f71.json
# MAGIC Almeta_Anderson_303f7023-ac56-5dc3-3dd0-363711e80272.json
# MAGIC Alphonso_Walter_ebdc7ccd-8b4e-1191-573c-bcf2c775926e.json
# MAGIC Althea_Gerlach_3fb90ed5-5b9f-5d23-202f-aac37c210e51.json
# MAGIC Alton_Rutherford_2f6b1d91-dcf9-bf4c-0a6a-4029788ced1f.json
# MAGIC Alyse_Sipes_efee780e-f2b5-0701-9ac5-1229e699ac56.json
# MAGIC Amado_Harris_cb92aa38-b094-ace7-0ad3-c78dae29684c.json
# MAGIC Amberly_Wiegand_c0c683b4-ab86-55f0-ce33-2b5004aa0776.json
# MAGIC Ana_Marroquín_633c3110-cc82-6896-b25d-55042417d37a.json
# MAGIC Andreas_Botsford_f6828403-f23a-cf17-8bec-5a6657001282.json
# MAGIC Andreas_Wyman_0508f693-9ee2-04bb-6af7-2a5b778ac470.json
# MAGIC Andree_Schroeder_8bcee9fa-f0a8-9bd0-c91c-7ab64a05dd11.json
# MAGIC Andrew_Kohler_6821e560-074c-9fc0-478b-79ea9a401f49.json
# MAGIC Andria_Kautzer_d06359a6-c118-e729-e95b-bb9fea87eb72.json
# MAGIC Andrés_Echevarría_a343c014-b1a0-b4ea-cdfe-da751e9584e1.json
# MAGIC Andrés_Pichardo_85e7ca20-3d20-ec05-1b72-068e4cb1f9e2.json
# MAGIC Andrés_Ávila_45e4e8c6-6499-5d19-b3b8-574b39586600.json
# MAGIC Andy_Hegmann_c67dfe8f-08ca-7b57-3a81-1fb85fdd5b58.json
# MAGIC Angel_Waelchi_de102093-b3ee-99e6-87b7-149b105808c0.json
# MAGIC Angele_Fadel_0ca625df-1bfe-2882-b1c8-6ee1ab57aef1.json
# MAGIC Angelic_Goldner_6aba89bd-ebda-85bf-f593-efec7a4ed77b.json
# MAGIC Angla_Hammes_731bc439-c85a-5cf6-159d-e1edb5608a46.json
# MAGIC Anitra_Green_af6cbaa5-a5b2-a930-c6a6-fb9237ee6449.json
# MAGIC Anjanette_Price_2d1ca1fb-f0ce-5a8e-4054-aa525ea74851.json
# MAGIC Ann_Lemke_e23694a7-f6df-4702-af12-890b0ecedcb7.json
# MAGIC Anneliese_Kirlin_69561763-1d92-8c63-1bc9-168e00131817.json
# MAGIC Annie_Kihn_98ab785e-5965-34a9-44f2-92aaa674cf87.json
# MAGIC Antione_Lind_a89b269d-e118-a01d-eafa-7945a47cf284.json
# MAGIC Antoine_Lowe_ec737983-71be-f986-f2cc-ff9c17252902.json
# MAGIC Anton_Jenkins_8e7e8337-97cd-3bc4-6310-3b82057e9b85.json
# MAGIC Antonio_Hand_3a3dbb6b-df1c-0e88-32cc-ed413e8ebd3a.json
# MAGIC Antonio_Kling_0961ac77-8814-5f84-41ef-ce9b0ad44248.json
# MAGIC April_Brakus_a49bf5e5-e3f6-888d-ae9b-569105298466.json
# MAGIC Ara_Bailey_c38101e4-2ffb-ab2b-d33e-f67e644f97d8.json
# MAGIC Aretha_Blick_59136567-6de6-1bc4-d569-4bc08565e3d8.json
# MAGIC Ariane_Gorczany_9ad9d8b6-2b1c-781e-1305-b00ee248514b.json
# MAGIC Ariel_Gusikowski_f4fc8366-d949-f6b0-98a7-0f4de3958094.json
# MAGIC Ariel_Halvorson_5ba1da18-d081-62e4-9707-db747158fcb9.json
# MAGIC Arleen_Vandervort_281041ab-a4b8-d1a2-8fb3-b92b6d3e5b19.json
# MAGIC Arnold_Kris_dccc4348-67f9-2637-c727-3db255d206f7.json
# MAGIC Arnoldo_Anderson_d8f93767-a579-44d5-6b39-a13cc56c0522.json
# MAGIC Aron_Homenick_9cf8253a-2386-f620-8d85-f4685e346e62.json
# MAGIC Art_Wiza_03b61c02-7906-898d-6826-372dc3bf7a7e.json
# MAGIC Arthur_Terry_b8405168-983e-1230-c759-7bcf27cb286e.json
# MAGIC Arturo_Corrales_83fe8cc7-c91c-fa3e-f10a-098422c12621.json
# MAGIC Arturo_Gutmann_321797fe-d48b-deff-6c5b-d8075bd8d551.json
# MAGIC Ashlea_Kutch_8ee1d688-d41f-9b87-1812-c60bf15af4df.json
# MAGIC Ashlee_Daugherty_55d6e93e-9845-090d-27af-46e1631329d7.json
# MAGIC Ashly_Swaniawski_9bfa709d-2e66-e15d-1d75-01ddd5d0b011.json" > files.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cat $VOLUME_PATH/files.txt

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC # uncomment for proxy
# MAGIC # export https_proxy=inetgate.highmark.com:9121
# MAGIC # export http_proxy=inetgate.highmark.com:9121
# MAGIC
# MAGIC cd $VOLUME_PATH;
# MAGIC mkdir fhir;
# MAGIC cd fhir;
# MAGIC
# MAGIC while read line; do 
# MAGIC curl -vvv -X GET  "http://hls-eng-data-public.s3.amazonaws.com/data/synthea/fhir/fhir/${line}" > $VOLUME_PATH/fhir/${line}; done < $VOLUME_PATH/files.txt
