FROM python:3.9.10

WORKDIR '/usr/src/app'

RUN apt-get update && \
    apt-get -y upgrade

RUN pip install pipenv

COPY ./Pipfile .

RUN pipenv install

COPY . .

CMD ["pipenv", "run", "uvicorn", "staged:app", "--reload", "--host", "0.0.0.0"]
