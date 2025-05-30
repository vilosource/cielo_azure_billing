# Writing Django Management Commands

This project keeps management commands small and focused. Business logic should
live in service classes so commands simply parse input and delegate work. The
`import_cost_csv` command is a good example.

## Steps to create a new command

1. Add a module under `billing/management/commands/` named after your command.
2. Define a `Command` class that inherits from `BaseCommand`.
3. Implement `add_arguments` to declare CLI options.
4. In `handle`, create or call a dedicated service class that performs the
   operation.

```python
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "Describe what your command does"

    def add_arguments(self, parser):
        parser.add_argument("--option", help="example option")

    def handle(self, *args, **options):
        service = MyService(options["option"])
        service.run()
        self.stdout.write(self.style.SUCCESS("Command completed"))
```

## Design principles

- **Single Responsibility** – keep command classes thin; heavy logic belongs in
  services or domain objects.
- **Open/Closed** – extend behaviour via new services instead of modifying
  existing commands.
- **Dependency Inversion** – services should depend on abstractions or
  configuration providers, making them easier to test.
- **Command Pattern** – if a command grows complex, break steps into smaller
  command objects that can be composed or reused.
- **Logging** – use the `logging` module inside services to report progress and
  errors.

Following these guidelines makes commands easier to test and maintain while
keeping the application modular.
