import asyncio
from playwright.async_api import async_playwright

class BrowserManager:
    """Gestiona el ciclo de vida del navegador de forma eficiente"""
    
    def __init__(self):
        self.playwright = None
        self.browser = None
        self._ref_count = 0
        
    async def start(self):
        """Inicia el navegador si no está activo"""
        if self.browser is None:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--disable-setuid-sandbox',
                    '--no-first-run',
                    '--no-sandbox',
                    '--no-zygote',
                    '--single-process',
                    '--max-old-space-size=2048'
                ]
            )
        self._ref_count += 1
        return self.browser
        
    async def stop(self):
        """Cierra el navegador cuando no hay más referencias"""
        self._ref_count -= 1
        if self._ref_count <= 0 and self.browser:
            await self.browser.close()
            await self.playwright.stop()
            self.browser = None
            self.playwright = None
            
    async def new_context(self, **kwargs):
        """Crea un nuevo contexto con configuración optimizada"""
        browser = await self.start()
        default_kwargs = {
            'user_agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36",
            'viewport': {"width": 1280, "height": 900},
            'ignore_https_errors': True
        }
        default_kwargs.update(kwargs)
        context = await browser.new_context(**default_kwargs)
        
        # Bloquear recursos innecesarios
        await context.route("**/*.{png,jpg,jpeg,gif,svg,webp}", lambda route: route.abort())
        await context.route("**/*.css", lambda route: route.abort())
        await context.route("**/*.woff", lambda route: route.abort())
        await context.route("**/*.woff2", lambda route: route.abort())
        await context.route("**/*.ttf", lambda route: route.abort())
        
        return context

# Instancia global del gestor de navegadores
browser_manager = BrowserManager()