# Generated by Django 5.2.1 on 2025-05-30 12:39

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('billing', '0002_blob_source_base_folder'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='ImportSnapshot',
            new_name='CostReportSnapshot',
        ),
    ]
