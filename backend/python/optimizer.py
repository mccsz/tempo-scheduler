import math
import random
import re
import time


def validate_input(raw):
    if not isinstance(raw, dict):
        raise ValueError('Вкажіть параметри розкладу.')
    resources = raw.get('resources')
    tasks = raw.get('tasks')
    if not isinstance(resources, list) or not 1 <= len(resources) <= 8:
        raise ValueError('Потрібно від 1 до 8 виконавців.')
    if not isinstance(tasks, list) or not 2 <= len(tasks) <= 80:
        raise ValueError('Потрібно від 2 до 80 задач.')

    def identifier(value):
        text = str(value)
        if not 1 <= len(text) <= 40 or not all((char.isalnum() or char in '_-' for char in text)):
            raise ValueError('Некоректний ідентифікатор.')
        return text

    normalized_resources = []
    resource_ids = set()
    for index, resource in enumerate(resources):
        if not isinstance(resource, dict):
            raise ValueError('Некоректні дані виконавця.')
        resource_id = identifier(resource.get('id', f'r{index + 1}'))
        if resource_id in resource_ids:
            raise ValueError('Ідентифікатори виконавців мають бути унікальними.')
        resource_ids.add(resource_id)
        normalized_resources.append(
            {
                'id': resource_id,
                'name': str(resource.get('name') or f'Виконавець {index + 1}').strip()[:100],
            }
        )
    normalized_tasks = []
    task_ids = set()
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError('Некоректні дані задачі.')
        task_id = identifier(task.get('id', f't{index + 1}'))
        if task_id in task_ids:
            raise ValueError('Ідентифікатори задач мають бути унікальними.')
        task_ids.add(task_id)
        duration, deadline = (float(task.get('duration', 0)), float(task.get('deadline', 1000)))
        if not math.isfinite(duration) or not 0.001 <= duration <= 1000:
            raise ValueError('Тривалість задачі: від 0,001 до 1000 годин.')
        if not math.isfinite(deadline) or not 0.001 <= deadline <= 100000:
            raise ValueError('Дедлайн задачі: від 0,001 до 100 000 годин.')
        dependencies = task.get('dependencies', [])
        eligible = task.get('eligibleResources', [])
        if not isinstance(dependencies, list) or not isinstance(eligible, list):
            raise ValueError('Залежності та ресурси мають бути списками.')
        dependencies = list(dict.fromkeys(map(str, dependencies)))
        eligible = list(dict.fromkeys(map(str, eligible))) or [
            resource['id'] for resource in normalized_resources
        ]
        if not set(eligible) <= resource_ids:
            raise ValueError('Задача містить невідомого виконавця.')
        normalized_tasks.append(
            {
                'id': task_id,
                'name': str(task.get('name') or f'Задача {index + 1}').strip()[:100],
                'duration': math.floor(duration * 1000 + 0.5) / 1000,
                'deadline': math.floor(deadline * 1000 + 0.5) / 1000,
                'dependencies': dependencies,
                'eligibleResources': eligible,
            }
        )
    task_map = {task['id']: task for task in normalized_tasks}
    visiting, visited = (set(), set())

    def visit(task_id):
        if task_id in visiting:
            raise ValueError('Залежності утворюють цикл.')
        if task_id in visited:
            return
        visiting.add(task_id)
        for dependency in task_map[task_id]['dependencies']:
            if dependency not in task_map:
                raise ValueError('Залежність від невідомої задачі.')
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in task_map:
        visit(task_id)
    iterations, seed = (float(raw.get('iterations', 100000)), float(raw.get('seed', 42)))
    if (
        not math.isfinite(iterations)
        or not iterations.is_integer()
        or (not 1000 <= iterations <= 2000000)
    ):
        raise ValueError('Кількість ітерацій: від 1 000 до 2 000 000.')
    if not math.isfinite(seed) or not seed.is_integer() or abs(seed) > 9007199254740991:
        raise ValueError('Seed має бути цілим числом.')
    timing = {}
    if 'startTime' in raw or 'timeDisplay' in raw:
        timing = {
            'startTime': raw.get('startTime', '09:00'),
            'timeDisplay': raw.get('timeDisplay', 'clock'),
        }
        if not isinstance(timing['startTime'], str) or not re.fullmatch(
            '([01][0-9]|2[0-3]):[0-5][0-9]', timing['startTime']
        ):
            raise ValueError('Початок плану має бути у форматі ГГ:ХХ.')
        if timing['timeDisplay'] not in ('clock', 'hours'):
            raise ValueError('Невідомий формат часу.')
    return {
        'name': str(raw.get('name') or 'Новий розклад').strip()[:100],
        'resources': normalized_resources,
        'tasks': normalized_tasks,
        'iterations': int(iterations),
        'seed': int(seed),
        **timing,
    }


def optimize(raw, on_progress=None, is_cancelled=None):
    data = validate_input(raw)
    started = time.monotonic()
    randomizer = random.Random(data['seed'])
    tasks, resources = (data['tasks'], data['resources'])
    task_count = len(tasks)
    task_indices = {task['id']: index for index, task in enumerate(tasks)}
    resource_indices = {resource['id']: index for index, resource in enumerate(resources)}
    durations = [int(math.floor(task['duration'] * 1000 + 0.5)) for task in tasks]
    deadlines = [int(math.floor(task['deadline'] * 1000 + 0.5)) for task in tasks]
    dependencies = [
        [task_indices[dependency] for dependency in task['dependencies']] for task in tasks
    ]
    dependency_masks = [
        sum((1 << dependency for dependency in required)) for required in dependencies
    ]
    task_bits = [1 << index for index in range(task_count)]
    allowed_resources = [
        [resource_indices[item] for item in task['eligibleResources']] for task in tasks
    ]
    task_order = list(range(task_count))
    resource_assignment = [options[0] for options in allowed_resources]

    def evaluate(priority, assignment, capture=False, greedy=False):
        pending = priority.copy()
        task_ends = [0] * task_count
        resource_ends = [0] * len(resources)
        if capture:
            task_starts, assigned_resources = ([0] * task_count, [0] * task_count)
        else:
            task_starts, assigned_resources = (None, None)
        completed_mask = 0
        tardiness = 0
        while pending:
            for position, selected in enumerate(pending):
                if dependency_masks[selected] & completed_mask == dependency_masks[selected]:
                    break
            pending.pop(position)
            if greedy:
                resource = min(allowed_resources[selected], key=resource_ends.__getitem__)
            else:
                resource = assignment[selected]
            if greedy:
                assignment[selected] = resource
            dependency_end = max((task_ends[item] for item in dependencies[selected]), default=0)
            start = max(dependency_end, resource_ends[resource])
            end = start + durations[selected]
            task_ends[selected] = end
            resource_ends[resource] = end
            tardiness += max(0, end - deadlines[selected])
            completed_mask |= task_bits[selected]
            if capture:
                task_starts[selected], assigned_resources[selected] = (start, resource)
        makespan = max(resource_ends)
        score = makespan + 2 * tardiness
        if not capture:
            return score
        schedule = [
            {
                'taskId': task['id'],
                'name': task['name'],
                'resourceId': resources[assigned_resources[index]]['id'],
                'start': task_starts[index] / 1000,
                'end': task_ends[index] / 1000,
                'duration': task['duration'],
                'deadline': task['deadline'],
                'late': max(0, task_ends[index] - deadlines[index]) / 1000,
            }
            for index, task in enumerate(tasks)
        ]
        schedule.sort(key=lambda row: (row['start'], row['taskId']))
        return {
            'schedule': schedule,
            'makespan': makespan / 1000,
            'tardiness': tardiness / 1000,
            'score': score / 1000,
        }

    def report(progress, stage):
        if is_cancelled and is_cancelled():
            raise RuntimeError('TASK_CANCELLED')
        if on_progress:
            on_progress(progress, stage)

    report(3, 'Побудова початкового розкладу')
    baseline = evaluate(task_order, resource_assignment, True, True)
    current_score = best_score = int(math.floor(baseline['score'] * 1000 + 0.5))
    best_task_order, best_assignment = (task_order.copy(), resource_assignment.copy())
    convergence = [{'iteration': 0, 'score': baseline['score']}]
    iterations = data['iterations']
    report_every, cycle = (max(500, iterations // 100), max(1000, iterations // 5))
    for iteration in range(1, iterations + 1):
        first_index, second_index = (
            randomizer.randrange(task_count),
            randomizer.randrange(task_count),
        )
        if first_index == second_index:
            second_index = (second_index + 1) % task_count
        change_assignment = randomizer.random() < 0.35 and len(allowed_resources[first_index]) > 1
        previous_resource = resource_assignment[first_index]
        if change_assignment:
            resource_assignment[first_index] = randomizer.choice(allowed_resources[first_index])
        else:
            task_order[first_index], task_order[second_index] = (
                task_order[second_index],
                task_order[first_index],
            )
        candidate = evaluate(task_order, resource_assignment)
        temperature = max(
            0.001, baseline['score'] * 1000 * 0.06 * (1 - iteration % cycle / cycle) ** 3
        )
        score_difference = candidate - current_score
        if score_difference <= 0 or randomizer.random() < math.exp(-score_difference / temperature):
            current_score = candidate
            if candidate < best_score:
                best_score, best_task_order, best_assignment = (
                    candidate,
                    task_order.copy(),
                    resource_assignment.copy(),
                )
        elif change_assignment:
            resource_assignment[first_index] = previous_resource
        else:
            task_order[first_index], task_order[second_index] = (
                task_order[second_index],
                task_order[first_index],
            )
        if iteration % report_every == 0 or iteration == iterations:
            convergence.append({'iteration': iteration, 'score': best_score / 1000})
            report(
                min(97, 5 + iteration * 92 // iterations),
                f'Пошук кращого розкладу · {iteration:,} / {iterations:,} ітерацій',
            )
    optimized = evaluate(best_task_order, best_assignment, True)
    utilization = []
    for resource in resources:
        busy = sum(
            (
                row['duration']
                for row in optimized['schedule']
                if row['resourceId'] == resource['id']
            )
        )
        utilization.append(
            {
                'resourceId': resource['id'],
                'name': resource['name'],
                'busy': round(busy, 3),
                'utilization': round(busy / optimized['makespan'] * 100, 3),
            }
        )
    report(99, 'Формування діаграми та звіту')
    return {
        'baseline': baseline,
        'optimized': optimized,
        'improvementPercent': round(
            max(0, (baseline['score'] - optimized['score']) / baseline['score'] * 100), 3
        ),
        'iterations': iterations,
        'elapsedMs': round((time.monotonic() - started) * 1000),
        'resourceUtilization': utilization,
        'convergence': convergence,
        'algorithm': 'Simulated annealing + list scheduling (Python)',
    }
