from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0003_rename_importsnapshot_to_costreportsnapshot"),
    ]

    operations = [
        migrations.AddField(
            model_name="costreportsnapshot",
            name="status",
            field=models.CharField(
                max_length=20,
                choices=[
                    ("in_progress", "in_progress"),
                    ("complete", "complete"),
                    ("failed", "failed"),
                ],
                default="in_progress",
                db_index=True,
            ),
        ),
    ]
