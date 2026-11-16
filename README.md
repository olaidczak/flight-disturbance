# Flight Disturbance

Projekt zbierający dane o lotach, pogodzie, pogodzie kosmicznej i zdarzeniach sejsmicznych.

Tworzylem juz wspolnego bucketa chlopaki!!
uzywajcie tej nazwy bucket = "flight-disruption-tfstate-592964949672"
Jest juz wpisana w main.tf, ale tak tylko mowie.

(to byla komenda ktorej uzywalem:
aws s3api create-bucket --bucket flight-disruption-tfstate-592964949672 --region eu-north-1 \
  --create-bucket-configuration LocationConstraint=eu-north-1
aws s3api put-bucket-versioning --bucket flight-disruption-tfstate-592964949672 \
  --versioning-configuration Status=Enabled
  )