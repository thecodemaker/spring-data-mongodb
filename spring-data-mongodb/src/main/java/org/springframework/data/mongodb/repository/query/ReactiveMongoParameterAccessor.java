/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.repository.query;

import org.springframework.core.convert.ConversionService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import rx.Observable;
import rx.Single;

/**
 * @author Mark Paluch
 */
class ReactiveMongoParameterAccessor extends MongoParametersParameterAccessor {

	private final Object[] values;
	private MonoProcessor<?>[] subscribable;

	public ReactiveMongoParameterAccessor(MongoQueryMethod method, Object[] values, ConversionService conversionService) {

		super(method, values);

		this.values = values;
		this.subscribable = new MonoProcessor<?>[values.length];

		for (int i = 0; i < values.length; i++) {

			// TODO: Cleanup this hack to support absence of RxJava.
			if (values[i] instanceof Observable) {
				values[i] = conversionService.convert(values[i], Flux.class);
			}

			if (values[i] instanceof Single) {
				values[i] = conversionService.convert(values[i], Mono.class);
			}

			if (values[i] instanceof Flux) {
				subscribable[i] = ((Flux) values[i]).collectList().subscribe();
			}

			if (values[i] instanceof Mono) {
				subscribable[i] = ((Mono) values[i]).subscribe();
			}
		}
	}

	@Override
	protected <T> T getValue(int index) {

		if (subscribable[index] != null) {
			return (T) subscribable[index].block();
		}

		return super.getValue(index);
	}

	@Override
	public Object[] getValues() {

		Object[] result = new Object[values.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = getValue(i);
		}
		return result;
	}

	/*
		* (non-Javadoc)
		* @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
		*/
	public Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}
}
