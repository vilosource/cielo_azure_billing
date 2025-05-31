from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('billing', '0005_alter_costreportsnapshot_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='resource',
            name='resource_name',
            field=models.CharField(max_length=255, null=True, blank=True),
        ),
    ]
