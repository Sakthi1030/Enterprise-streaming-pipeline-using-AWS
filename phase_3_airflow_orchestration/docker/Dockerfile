FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Install Python packages
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" \
    && unzip /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install --bin-dir /home/airflow/.local/bin --install-dir /home/airflow/.local/aws-cli \
    && rm -rf /tmp/awscliv2.zip /tmp/aws

# Install SnowSQL
RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.27-linux_x86_64.bash \
    && SNOWSQL_DEST=/home/airflow/.local/bin SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.27-linux_x86_64.bash \
    && rm snowsql-1.2.27-linux_x86_64.bash

# Set PATH
ENV PATH="/home/airflow/.local/bin:${PATH}"

ENTRYPOINT ["/entrypoint.sh"]