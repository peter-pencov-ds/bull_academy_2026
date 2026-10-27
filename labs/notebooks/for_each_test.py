# Databricks notebook source
table = dbutils.widgets.get("input")

display(spark.sql(f'select "{table}", count(*) from interns.peter_pencov.{table}'))