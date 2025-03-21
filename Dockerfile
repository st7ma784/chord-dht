FROM st7ma784/superdarn_rstbase:latest 

# Install dependencies
#RUN apt install libffi-dev openssl-dev build-base python3-dev git

RUN apt-get install -y build-essential

# Install python dependencies
RUN pip install -U pip
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

# Copy the rest of the project, so the above is cached if requirements didn't change
COPY ./ /app/
EXPOSE 8001
EXPOSE 6501
CMD ["python3", "-u", "src/main.py", "--start-api"]
