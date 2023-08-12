FROM python:3.9
WORKDIR /app
COPY . /app

#RUN pip install --upgrade pip
#RUN pip install mysql-connector-python
RUN pip3 --no-cache-dir install -r requirements.txt

ENTRYPOINT ["python3"]
CMD ["catman_demo_app.py"]
#CMD ["python","app.py"]
