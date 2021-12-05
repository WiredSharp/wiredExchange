from python:3

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./wired_exchange wired_exchange/
COPY ./streamlit .

ENV STREAMLIT_SERVER_PORT=80
EXPOSE 80

ENTRYPOINT ["streamlit", "run"]
CMD ["wallet.py"]