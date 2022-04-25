 Access bash terminal for container:

 `docker exec -it es01 bash`

Access the CA fingerprint for the container:

`docker exec -it es01 openssl x509 -fingerprint -sha256 -in config/certs/es01/es01.crt`