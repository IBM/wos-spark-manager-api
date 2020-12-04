#!/bin/sh

gunicorn --config=service/gunicorn_config.py service.sw_app:app
