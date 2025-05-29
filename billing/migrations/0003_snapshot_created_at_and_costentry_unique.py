from django.db import migrations, models
import datetime

class Migration(migrations.Migration):

    dependencies = [
        ('billing', '0002_add_filter_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='importsnapshot',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=datetime.datetime.now),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name='costentry',
            unique_together={('snapshot', 'date', 'subscription', 'resource', 'meter', 'quantity', 'unit_price')},
        ),
    ]

