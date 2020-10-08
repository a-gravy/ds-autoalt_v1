FROM harbor.unext.jp/datascience-dev/ds-infra-kubebase:latest

WORKDIR /app/

# TODO Increment the number to force-refresh the lower layers.
RUN echo '6' > update_me && rm update_me

COPY pip.conf /etc/pip.conf
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /app/

RUN python  setup.py  sdist  bdist_wheel && pip install dist/*.whl

# Clean up
# RUN rm -r /app/target

# a way to communicate with airflow
ENTRYPOINT ["./entrypoint.sh"]