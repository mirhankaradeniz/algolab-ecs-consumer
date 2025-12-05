# Algolab ECS Consumer (WSS D Paket Abonesi)

## 1) Yapı
- `consumer.py`: WSS'e bağlanır, H/D mesajı gönderir, D verisini DynamoDB'ye yazar.
- `Dockerfile`: Basit Python container.
- `taskdef.json`: Fargate Task Definition iskeleti.

## 2) Gerekli Environment
- API_HOSTNAME, WSS_ENDPOINT, DDB_LOGIN_TABLE, DDB_FEED_TABLE, ACCOUNT_NAME, SYMBOLS, AWS_REGION, API_KEY

## 3) Build & Push (Git şart değil)
```bash
AWS_REGION=eu-west-3
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws ecr create-repository --repository-name algolab-consumer --region $AWS_REGION || true
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

docker build -t algolab-consumer:latest .
docker tag algolab-consumer:latest $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/algolab-consumer:latest
docker push $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/algolab-consumer:latest
