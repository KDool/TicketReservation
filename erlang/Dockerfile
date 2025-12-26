FROM erlang:26
WORKDIR /app

COPY app/reservation_core/src/ /app/reservation_core/src/
COPY entrypoint.sh /app/entrypoint.sh
COPY init_mnesia.erl /app/init_mnesia.erl

RUN chmod +x /app/entrypoint.sh \
 && erlc -o /app /app/init_mnesia.erl \
 && erlc -o /app /app/reservation_core/src/*.erl

ENTRYPOINT ["/app/entrypoint.sh"]
