#!/usr/bin/env python3
import subprocess
import random
import time
import argparse
import sys
from typing import List

def get_compose_containers() -> List[str]:
    try:
        result = subprocess.run(
            ['docker', 'compose', 'ps', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            check=True
        )
        return [name.strip() for name in result.stdout.strip().split('\n') if name.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Error executing docker compose ps: {e}")
        return []


def filter_worker_containers(containers: List[str], excluded_patterns: List[str] = None) -> List[str]:
    if excluded_patterns is None:
        excluded_patterns = []
    
    # Always exclude rabbitmq
    excluded_patterns = list(set(excluded_patterns + ['rabbitmq']))
    
    workers = []

    for container in containers:
        if any(pattern in container for pattern in excluded_patterns):
            continue

        workers.append(container)

    return workers


def kill_container(container_name: str) -> bool:
    try:
        # Stop the container
        result = subprocess.run(
            ['docker', 'stop', container_name],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Container {container_name} stopped | result {result.returncode}")
        
        # Kill the container
        result = subprocess.run(
            ['docker', 'kill', container_name],
            capture_output=True,
            text=True,
            check=False
        )
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error stopping container {container_name}: {e.stderr}")
        return False


def chaos_monkey_loop(interval: int = 5, excluded_patterns: List[str] = None):
    print(f"Chaos Monkey started")
    print(f"Interval: {interval} seconds")
    if excluded_patterns:
        print(f"Excluded patterns: {excluded_patterns + ['rabbitmq']}")
    else:
        print(f"Excluded patterns: ['rabbitmq']")
    print(f"Press Ctrl+C to stop\n")
    
    try:
        while True:
            time.sleep(interval)
            containers = get_compose_containers()
            workers = filter_worker_containers(containers, excluded_patterns)

            if not workers:
                print(f"No workers found")
                continue
            
            victim = random.choice(workers)
            
            print(f"[{time.strftime('%H:%M:%S')}] Number of available workers: {len(workers)}")
            print(f"Selected victim: {victim}")
            
            kill_container(victim)
            
    except KeyboardInterrupt:
        print(f"\nChaos Monkey stopped")


def main():
    parser = argparse.ArgumentParser(
        description='Chaos Monkey'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Interval in seconds between each attack (default: 5)'
    )
    parser.add_argument(
        '--exclude',
        type=str,
        nargs='+',
        default=[],
        help='Pattern containers to exclude from chaos attacks (rabbitmq is always excluded). Example: --exclude client gateway'
    )
    
    args = parser.parse_args()
    
    chaos_monkey_loop(interval=args.interval, excluded_patterns=args.exclude)


if __name__ == '__main__':
    main()