from django.contrib import admin
from .models import IPCData

@admin.register(IPCData)
class IPCDataAdmin(admin.ModelAdmin):
    list_display = ['periodo', 'fecha', 'variacion_mensual', 'variacion_anual', 'updated_at']
    list_filter = ['fecha', 'updated_at']
    search_fields = ['periodo']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['-fecha']
    list_per_page = 25
    
    fieldsets = (
        ('Información Principal', {
            'fields': ('periodo', 'fecha')
        }),
        ('Datos IPC', {
            'fields': ('variacion_mensual', 'variacion_anual'),
            'description': 'Variaciones porcentuales del IPC'
        }),
        ('Metadatos', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def get_readonly_fields(self, request, obj=None):
        if obj:  # Editando objeto existente
            return self.readonly_fields + ['periodo', 'fecha']
        return self.readonly_fields

# Personalizar el sitio admin
admin.site.site_header = "Panel de Control - Analytics Platform"
admin.site.site_title = "Analytics Platform"
admin.site.index_title = "Administración de Datos Económicos"