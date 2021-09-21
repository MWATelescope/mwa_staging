FROM python:3.8

WORKDIR '/usr/src/app'

RUN apt-get update && \
    apt-get -y upgrade

RUN pip install pipenv

COPY ./Pipfile .
COPY ./Pipfile.lock .

RUN pipenv install --deploy --ignore-pipfile

COPY . .

EXPOSE 8000

CMD ["pipenv", "run", "uvicorn", "staged:app", "--host", "0.0.0.0"]