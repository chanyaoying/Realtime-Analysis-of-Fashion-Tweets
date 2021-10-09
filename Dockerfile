FROM python:3.9.5

WORKDIR /home
ADD . /home
RUN pip install -r requirements.txt
