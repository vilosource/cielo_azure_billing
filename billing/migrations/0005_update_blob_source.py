from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0004_add_blob_source_and_update_snapshot"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="billingblobsource",
            name="subscription",
        ),
        migrations.AlterUniqueTogether(
            name="billingblobsource",
            unique_together=set(),
        ),
        migrations.AlterModelOptions(
            name="billingblobsource",
            options={"ordering": ["name"]},
        ),
    ]
