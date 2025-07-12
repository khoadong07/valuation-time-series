from app.core.training import train_and_forecast_by_authority
from app.services.utils import search_external_data_by_local_authority


local_authority = "Adur"

async def main():
    get_extra_data = await search_external_data_by_local_authority(local_authority)

    result = await train_and_forecast_by_authority(get_extra_data)
    print(result)
    # do something with get_extra_data

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())