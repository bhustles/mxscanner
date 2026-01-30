"""Quick DNS test script"""
import dns.resolver

domains = ['boreal.org', 'premierinc.com', 'jwi.org', 'nate.com']

print("Testing DNS resolution with Google DNS (8.8.8.8)...")
print("-" * 50)

for domain in domains:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ['8.8.8.8']
    resolver.timeout = 5
    resolver.lifetime = 5
    try:
        answers = resolver.resolve(domain, 'MX')
        mx_list = [str(r.exchange).rstrip('.') for r in answers]
        print(f'{domain}: OK - {mx_list}')
    except dns.resolver.NXDOMAIN:
        print(f'{domain}: NXDOMAIN (domain not found)')
    except dns.resolver.NoAnswer:
        print(f'{domain}: No MX records')
    except Exception as e:
        print(f'{domain}: Error - {type(e).__name__}: {e}')

print("-" * 50)
print("Done!")
