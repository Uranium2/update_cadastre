# Update cadastre

Update cadastre repository's job is to **check if a new cadastre is available and download it** from datagouv.fr with ```main.py```

This repository is pulled directly in our Data Achitecture on an **AWS EC2.**

```stop_instance.py``` script allows to the EC2 instance to **shutdown himself** with a AWS Lambda triggered by a AWS CloudWatch. 
