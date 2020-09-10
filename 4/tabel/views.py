from django.shortcuts import render
from django.views import View
from django.db import transaction
from django.db.models import Q, When, Case

from datetime import datetime

from .models import Clients, Durations, Modes, Equipment


def get_standard_date_time(date: str, time: str) -> datetime:
    return datetime.strptime(date + ' ' + time, '%Y-%m-%d %H:%M')

def get_standard_name(name: str) -> str:
    return '' if name == 'Все' else name

def get_base_context() -> dict:
    with transaction.atomic():
        clients = Clients.objects.all()
        modes = Modes.objects.all()
        equipments = Equipment.objects.all()
    context = {
        'clients': clients,
        'modes': modes,
        'equipments': equipments
    }
    return context

class TableView(View):
    template_name = 'tabel/tabel.html'

    def get(self, *args, **kwargs):
        if self.request.GET:
            client_name = self.request.GET.get('client', '')
            equipment_name = self.request.GET.get('equipment', '')
            mode_name = self.request.GET.get('mode', '')
            duration = self.request.GET.get('duration', 0)
            start_date = self.request.GET.get('start_date')
            end_date = self.request.GET.get('end_date')
            start_time = self.request.GET.get('start_time')
            end_time = self.request.GET.get('end_time')
            
            start = get_standard_date_time(start_date, start_time)
            end = get_standard_date_time(end_date, end_time)

            durations = Durations.objects.filter(
                Q(start__startswith=start) | Q(stop__endswith=end),
                client__name__contains=get_standard_name(client_name),
                equipment__name__contains=get_standard_name(equipment_name),
                mode__name__contains=get_standard_name(mode_name),
                minutes__lte=duration,
            )

            context = get_base_context()
            context['durations'] = durations

            return render(self.request, template_name=self.template_name, context=context)

        else:
            return render(self.request, template_name=self.template_name, context=get_base_context())

