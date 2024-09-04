if [ "$QUEUE_MODE" = "True" ]; then
        python main.py
else
        gunicorn --bind 0.0.0.0:8888 --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT main:app
fi
