from django.urls import reverse, resolve


def test_table_urls():
    assert reverse('tabel:table-view') == '/'
    assert resolve('/').view_name == 'tabel:table-view'

