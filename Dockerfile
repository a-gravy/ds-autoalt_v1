FROM harbor.unext.jp/datascience-dev/ds-infra-kubebase:latest

WORKDIR /app/

# TODO Increment the number to force-refresh the lower layers.
RUN echo '8' > update_me && rm update_me

RUN INSTALL_PATH=~/anaconda \
    && wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && bash Miniconda3-latest* -fbp $INSTALL_PATH
ENV PATH=/root/anaconda/bin:$PATH

# Updating Anaconda packages
RUN conda update conda \
    && conda update --all

COPY pip.conf /etc/pip.conf
COPY env.yml /app
RUN conda env create -f env.yml

# Pull the environment name out of the environment.yml
RUN echo "source activate $(head -1 env.yml | cut -d' ' -f2)" > ~/.bashrc
ENV PATH /opt/conda/envs/$(head -1 env.yml | cut -d' ' -f2)/bin:$PATH

# TODO Increment the number to force-refresh the lower layers.
RUN echo '4'

COPY *.py /app/
COPY bpr /app/bpr
COPY autoalts /app/autoalts
COPY config.yaml /app/


# Clean up, but we don't need this now
#RUN rm -r /app/target

# a way to communicate with airflow
ENTRYPOINT ["./entrypoint.sh"]
