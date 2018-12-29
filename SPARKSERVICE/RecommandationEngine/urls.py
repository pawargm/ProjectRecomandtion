"""SPARKSERVICE URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url,include
from django.contrib import admin
#from .views import 
#from .apiviews import  PollList, PollDetail ,StudentView,StudentDelete,StudentAdd
from . import views


urlpatterns = [

	url(r'recommandres/',views.get_recommandation,name = 'recommandation'),
	url(r'test/',views.response_springboot,name = 'response'),
	url(r'testarg/',views.response_springbootarg,name = 'response'),
    url(r'testsock/',views.try_websock,name = 'response'),
    #used URLS
    url(r'staledata/',views.delete_stale_result,name='response'),
    url(r'trainandprediction/',views.train_predict_model,name='response'),
    url(r'preditiononly/',views.predict_model,name='response')
    #delete_stale_result
    #url(r'stale_mode/',views.delete_stale_result,name='response'),
]
