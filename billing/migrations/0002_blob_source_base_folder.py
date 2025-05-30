from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="billingblobsource",
            name="base_folder",
            field=models.CharField(
                default="",
                max_length=255,
                help_text="Base export folder e.g. costreports/prod/prod-actual-cost/",
            ),
            preserve_default=False,
        ),
        migrations.RemoveField(
            model_name="billingblobsource",
            name="path_template",
        ),
        migrations.RemoveField(
            model_name="billingblobsource",
            name="guid",
        ),
    ]
